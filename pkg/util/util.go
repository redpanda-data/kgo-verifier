package util

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func Die(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	log.Error(formatted)
	os.Exit(1)
}

func Chk(err error, msg string, args ...interface{}) {
	if err != nil {
		Die(msg, args...)
	}
}

func MakeAcks(acks int) (kgo.Acks, error) {
	if acks == -1 {
		return kgo.AllISRAcks(), nil
	} else if acks == 1 {
		return kgo.LeaderAck(), nil
	} else if acks == 0 {
		return kgo.NoAck(), nil
	}
	return kgo.Acks{}, fmt.Errorf("Invalid acks %d, values supported is -1, 0, 1.", acks)
}
