package archive

import (
	"archive/tar"
	"fmt"
	"io"
	"io/fs"
	"os"
)

func addFileToWriter(fi fs.FileInfo, pathToFile, pathInTar string, tarWriter *tar.Writer) error {
	header, err := tar.FileInfoHeader(fi, fi.Name())
	if err != nil {
		return fmt.Errorf("%w", err)
	}
	header.Name = pathInTar

	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("%w", err)
	}
	// Open the file for reading
	file, err := os.Open(pathToFile)
	if err != nil {
		return fmt.Errorf("%w", err)
	}
	defer file.Close()

	// Copy the file contents to the tar archive
	if _, err := io.Copy(tarWriter, file); err != nil {
		return fmt.Errorf("%w", err)
	}
	return nil
}
