package kinetic

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	//gokinesis "github.com/rewardStyle/go-kinesis"
)

// ErrMetaAuthentication represents an error that occurred on authentication from meta
var ErrMetaAuthentication = errors.New("Authentication error: failed to auth from meta.  Your IAM roles are bad, or you need to specify an AccessKey and SecretKey")

func authenticate(accessKey, secretKey string) (sess *session.Session, err error) {
	if accessKey == "" || secretKey == "" {
		if sess, err = session.NewSession(); err != nil {
			return nil, ErrMetaAuthentication
		}
	} else {
		conf := &aws.Config{}
		conf = conf.WithCredentials(credentials.NewStaticCredentials(accessKey, secretKey, ""))
		sess, err = session.NewSessionWithOptions(session.Options{Config: *conf})
	}
	return

}
