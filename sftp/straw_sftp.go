package sftp

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
	"github.com/uw-labs/straw"
	"golang.org/x/crypto/ssh"
)

var _ straw.StreamStore = &sftpStreamStore{}

func init() {
	straw.Register("sftp", func(u *url.URL) (straw.StreamStore, error) {
		return newSFTPStreamStore(u.String())
	})
}

type sftpStreamStore struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func newSFTPStreamStore(urlString string) (*sftpStreamStore, error) {
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

	ss := &sftpStreamStore{client, sclient}

	return ss, nil
}

func (s *sftpStreamStore) Close() error {
	e1 := s.sftpClient.Close()
	e2 := s.sshClient.Close()
	if e1 != nil {
		return e1
	}
	return e2
}

func (s *sftpStreamStore) Lstat(filename string) (os.FileInfo, error) {
	return s.sftpClient.Lstat(filename)
}

func (s *sftpStreamStore) Stat(filename string) (os.FileInfo, error) {
	return s.sftpClient.Stat(filename)
}

func (s *sftpStreamStore) Mkdir(path string, mode os.FileMode) error {
	err := s.sftpClient.Mkdir(path)
	if err != nil && strings.Contains(err.Error(), ": file exists") {
		d, _ := filepath.Split(path)
		return fmt.Errorf("%s file exists", d)
	}
	return err
}

func (s *sftpStreamStore) OpenReadCloser(name string) (straw.StrawReader, error) {
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

func (s *sftpStreamStore) Remove(name string) error {
	err := s.sftpClient.Remove(name)
	if err != nil && strings.Contains(err.Error(), ": directory not empty") {
		return fmt.Errorf("%s directory not empty", name)
	}
	return err
}

func (s *sftpStreamStore) CreateWriteCloser(name string) (straw.StrawWriter, error) {
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

func (s *sftpStreamStore) Readdir(name string) ([]os.FileInfo, error) {
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

func (r *sftpReader) Seek(offset int64, whence int) (int64, error) {
	r.lk.Lock()
	defer r.lk.Unlock()

	return r.f.Seek(offset, whence)
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
