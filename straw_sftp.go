package straw

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

var _ StreamStore = &SFTPStreamStore{}

type SFTPStreamStore struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func NewSFTPStreamStore(urlString string) (*SFTPStreamStore, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	pass, passSet := u.User.Password()
	if u.User.Username() == "" || !passSet {
		return nil, errors.New("username and password are required in the url")
	}

	config := &ssh.ClientConfig{
		User: u.User.Username(),
		Auth: []ssh.AuthMethod{
			ssh.Password(pass),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // TODO: allow passing host key as url param. ssh.FixedHostKey(hostKey),
	}

	client, err := ssh.Dial("tcp", u.Host, config)
	if err != nil {
		return nil, err
	}

	sclient, err := sftp.NewClient(client)
	if err != nil {
		client.Close()
		return nil, err
	}

	ss := &SFTPStreamStore{client, sclient}

	return ss, nil
}

func (s *SFTPStreamStore) Lstat(filename string) (os.FileInfo, error) {
	return s.sftpClient.Lstat(filename)
}

func (s *SFTPStreamStore) Stat(filename string) (os.FileInfo, error) {
	return s.sftpClient.Stat(filename)
}

func (s *SFTPStreamStore) Mkdir(path string, mode os.FileMode) error {
	err := s.sftpClient.Mkdir(path)
	if err != nil && strings.Contains(err.Error(), ": file exists") {
		d, _ := filepath.Split(path)
		return fmt.Errorf("%s file exists", d)
	}
	return err
}

func (s *SFTPStreamStore) OpenReadCloser(name string) (StrawReader, error) {
	sr, err := s.sftpClient.Open(name)
	if err != nil {
		return nil, err
	}
	fi, err := sr.Stat()
	if err != nil {
		sr.Close()
		return nil, err
	}
	if fi.IsDir() {
		sr.Close()
		return nil, fmt.Errorf("%s is a directory", name)
	}
	return &sftpReader{f: sr}, nil
}

func (s *SFTPStreamStore) Remove(name string) error {
	err := s.sftpClient.Remove(name)
	if err != nil && strings.Contains(err.Error(), ": directory not empty") {
		return fmt.Errorf("%s directory not empty", name)
	}
	return err
}

func (s *SFTPStreamStore) CreateWriteCloser(name string) (StrawWriter, error) {
	fi, err := s.Stat(name)
	if err == nil && fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory", name)
	}

	sw, err := s.sftpClient.Create(name)
	if err != nil && strings.Contains(err.Error(), ": not a directory") {
		d, _ := filepath.Split(name)
		return nil, fmt.Errorf("%s not a directory", d)
	}
	return sw, nil
}

func (s *SFTPStreamStore) Readdir(name string) ([]os.FileInfo, error) {
	fi, err := s.sftpClient.ReadDir(name)
	if err != nil {
		return nil, err
	}
	sort.Slice(fi, func(i, j int) bool { return fi[i].Name() < fi[j].Name() })
	return fi, nil
}

type sftpReader struct {
	lk sync.Mutex
	f  *sftp.File
}

func (r *sftpReader) Close() error {
	r.lk.Lock()
	defer r.lk.Unlock()
	return r.f.Close()
}

func (r *sftpReader) Read(buf []byte) (int, error) {
	r.lk.Lock()
	defer r.lk.Unlock()
	return r.f.Read(buf)
}

func (r *sftpReader) ReadAt(buf []byte, offset int64) (int, error) {
	r.lk.Lock()
	defer r.lk.Unlock()
	// get current offset
	oldOffset, err := r.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	// seek to offset
	off, err := r.f.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}
	if off != offset {
		panic("bug in offset handling")
	}
	j, err := io.ReadFull(r.f, buf)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}

	// put the original offset back
	if _, err := r.f.Seek(oldOffset, io.SeekStart); err != nil {
		return j, err
	}

	return j, err
}
