package storage

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"path"
	"path/filepath"
)

type Storage interface {
	DownloadFile(src, dest string) error
	UploadFile(src, dest, perms string) error
	UploadDirectory(src, dest string, perms string) error
	FileExists(file string) (bool, error)
}

type s3Storage struct {
	awsSession *session.Session
	bucket     string
}

func (o *s3Storage) FileExists(file string) (bool, error) {
	sess := s3.New(o.awsSession)
	_, err := sess.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(file),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == "404" {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func (o *s3Storage) DownloadFile(src, dest string) error {

	// Make sure our download directory exists
	if err := os.MkdirAll(dest, 0700); err != nil {
		return err
	}
	logrus.WithFields(logrus.Fields{
		"m":         "storage",
		"src_file":  src,
		"dest_file": dest,
	}).Debug("download file")

	destFile := dest + "/" + path.Base(src)
	file, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer file.Close()

	downloader := s3manager.NewDownloader(o.awsSession)
	_, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(src),
	})
	return err
}

func (o *s3Storage) UploadFile(src, dest, perms string) error {
	logrus.WithFields(logrus.Fields{
		"pkg":       "services/storage",
		"src_file":  src,
		"dest_file": dest,
	}).Debug("upload file")

	file, err := os.Open(src)
	if err != nil {
		return err
	}
	defer file.Close()

	uploader := s3manager.NewUploader(o.awsSession)
	_, err = uploader.Upload(&s3manager.UploadInput{
		ACL:    aws.String(perms),
		Body:   file,
		Bucket: aws.String(o.bucket),
		Key:    aws.String(dest),
	})
	return err
}

func (o *s3Storage) UploadDirectory(src, dest, perms string) error {
	fileList := []string{}
	err := filepath.Walk(src, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, file := range fileList {
		o.UploadFile(file, fmt.Sprintf("%s/%s", dest, path.Base(file)), perms)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewS3(bucket string, session *session.Session) Storage {
	return &s3Storage{
		awsSession: session,
		bucket:     bucket,
	}
}
