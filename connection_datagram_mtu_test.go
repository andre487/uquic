package quic

import (
	"testing"

	"github.com/refraction-networking/uquic/internal/mocks"
	mockackhandler "github.com/refraction-networking/uquic/internal/mocks/ackhandler"
	"github.com/refraction-networking/uquic/internal/monotime"
	"github.com/refraction-networking/uquic/internal/protocol"
	"github.com/refraction-networking/uquic/internal/wire"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestConnectionDatagramBudgetAfterMTUDiscovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	sent := mockackhandler.NewMockSentPacketHandler(ctrl)
	crypto := mocks.NewMockCryptoSetup(ctrl)
	mtu := NewMockMTUDiscoverer(ctrl)
	tc := newServerTestConnection(t, ctrl, &Config{EnableDatagrams: true, InitialPacketSize: 1200}, false,
		connectionOptSentPacketHandler(sent), connectionOptCryptoSetup(crypto))
	c := tc.conn
	c.peerParams = &wire.TransportParameters{MaxDatagramFrameSize: 65535}
	c.mtuDiscoverer = mtu
	ack := &wire.AckFrame{AckRanges: []wire.AckRange{{Smallest: 1, Largest: 1}}}
	sent.EXPECT().ReceivedAck(ack, protocol.Encryption1RTT, gomock.Any()).Return(true, nil)
	mtu.EXPECT().CurrentSize().Return(protocol.ByteCount(1452))
	sent.EXPECT().SetMaxDatagramSize(protocol.ByteCount(1452))
	crypto.EXPECT().SetLargest1RTTAcked(protocol.PacketNumber(1)).Return(nil)
	require.NoError(t, c.handleAckFrame(ack, protocol.Encryption1RTT, monotime.Now()))

	var limit *DatagramTooLargeError
	require.ErrorAs(t, c.SendDatagram(make([]byte, 65536)), &limit)
	payload := make([]byte, int(limit.MaxDatagramPayloadSize))
	frame := &wire.DatagramFrame{Data: payload, DataLenPresent: true}
	packetLen := wire.ShortHeaderLen(c.connIDManager.Get(), protocol.PacketNumberLen4) + frame.Length(c.version) + 16
	require.LessOrEqual(t, packetLen, protocol.ByteCount(1452), "accepted payload must fit the discovered packet MTU")
	require.NoError(t, c.SendDatagram(payload))
	require.Equal(t, payload, c.datagramQueue.Peek().Data)
	require.Error(t, c.SendDatagram(make([]byte, len(payload)+1)))
}

func TestConnectionDatagramBudgetOnLocalMigration(t *testing.T) {
	ctrl := gomock.NewController(t)
	sent := mockackhandler.NewMockSentPacketHandler(ctrl)
	sender := NewMockSender(ctrl)
	mtu := NewMockMTUDiscoverer(ctrl)
	tc := newClientTestConnection(t, ctrl, nil, false,
		connectionOptSentPacketHandler(sent), connectionOptSender(sender))
	c := tc.conn
	c.peerParams = &wire.TransportParameters{MaxUDPPayloadSize: 1452}
	c.mtuDiscoverer = mtu
	c.currentPacketMTU.Store(1452)
	tr := &Transport{Conn: newUDPConnLocalhost(t)}
	require.NoError(t, tr.init(false))
	defer tr.Close()
	now := monotime.Now()
	initialMTU := protocol.ByteCount(c.config.InitialPacketSize)
	sent.EXPECT().MigratedPath(now, initialMTU)
	mtu.EXPECT().Reset(now, initialMTU, protocol.ByteCount(1452))
	sender.EXPECT().Close()
	c.switchToNewPath(tr, now)
	defer c.sendQueue.Close()
	require.Equal(t, uint32(initialMTU), c.currentPacketMTU.Load())
}
