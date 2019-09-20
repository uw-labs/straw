package straw

import (
	"errors"
	"os"
	"path/filepath"
)

// SkipDir is used as a return value from WalkFuncs to indicate that
// the directory named in the call is to be skipped. It is not returned
// as an error by any function.
var SkipDir = errors.New("skip this directory")

// WalkFunc is the type of the function called for each file or directory
// visited by Walk. The path argument contains the argument to Walk as a
// prefix; that is, if Walk is called with "dir", which is a directory
// containing the file "a", the walk function will be called with argument
// "dir/a". The info argument is the os.FileInfo for the named path.
//
// If there was a problem walking to the file or directory named by path, the
// incoming error will describe the problem and the function can decide how
// to handle that error (and Walk will not descend into that directory). In the
// case of an error, the info argument will be nil. If an error is returned,
// processing stops. The sole exception is when the function returns the special
// value SkipDir. If the function returns SkipDir when invoked on a directory,
// Walk skips the directory's contents entirely. If the function returns SkipDir
// when invoked on a non-directory file, Walk skips the remaining files in the
// containing directory.
type WalkFunc = func(string, os.FileInfo, error) error

func walk(store StreamStore, path string, info os.FileInfo, walkFn WalkFunc) error {

	if !info.IsDir() {
		return walkFn(path, info, nil)
	}

	fileInfos, err := store.Readdir(path)
	err1 := walkFn(path, info, err)

	if err != nil || err1 != nil {
		return err1
	}

	for _, fileInfo := range fileInfos {
		filename := filepath.Join(path, fileInfo.Name())
		err = walk(store, filename, fileInfo, walkFn)
		if err != nil {
			if !fileInfo.IsDir() || err != SkipDir {
				return err
			}
		}
	}

	return nil
}

// Walk walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root. All errors that arise visiting files
// and directories are filtered by walkFn. The files are walked in lexical
// order, which makes the output deterministic but means that for very
// large directories Walk can be inefficient.
// Walk does not follow symbolic links.
// This is the straw equivalent of filepath.Walk in the standard library.
func Walk(store StreamStore, root string, walkFn WalkFunc) error {
	info, err := store.Stat(root)
	if err != nil {
		err = walkFn(root, nil, err)
	} else {
		err = walk(store, root, info, walkFn)
	}
	if err == SkipDir {
		return nil
	}
	return err
}
