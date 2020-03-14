module s3sync

go 1.13

require (
	github.com/aws/aws-sdk-go v1.29.24
	github.com/google/uuid v1.1.1
	github.com/jmespath/go-jmespath v0.3.0 // indirect
	github.com/johannesboyne/gofakes3 v0.0.0-20191029185751-e238f04965fe
	github.com/sirupsen/logrus v1.4.2
)

replace github.com/johannesboyne/gofakes3 => github.com/mars4myshare/gofakes3 v0.0.0-20191226083417-0737d882e413
