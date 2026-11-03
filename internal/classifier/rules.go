package classifier

import (
	"bytes"
	"path/filepath"
	"strings"
)

// Category represents a sorted destination category directory.
type Category string

const (
	// CategoryVideos stores video formats (mp4, mkv, avi, mov, etc.).
	CategoryVideos Category = "Videos"
	// CategoryDocuments stores document formats (pdf, docx, xlsx, txt, etc.).
	CategoryDocuments Category = "Documents"
	// CategoryAudio stores sound and music formats (mp3, flac, wav, etc.).
	CategoryAudio Category = "Audio"
	// CategoryArchives stores compressed containers (zip, tar, 7z, rar, etc.).
	CategoryArchives Category = "Archives"
	// CategorySoftware stores executables, packages, and images (exe, dmg, deb, elf, etc.).
	CategorySoftware Category = "Software"
	// CategoryImages stores raster and vector graphics (png, jpg, gif, webp, etc.).
	CategoryImages Category = "Images"
	// CategoryMiscellaneous is the fallback directory for uncategorized formats.
	CategoryMiscellaneous Category = "Miscellaneous"
)

// magicRule checks if buffer matches a specific byte signature.
type magicRule struct {
	category Category
	match    func(buf []byte) bool
}

var magicRules = []magicRule{
	// PDF (%PDF-)
	{
		category: CategoryDocuments,
		match: func(b []byte) bool {
			return len(b) >= 5 && bytes.HasPrefix(b, []byte("%PDF-"))
		},
	},
	// PNG (\x89PNG\r\n\x1a\n)
	{
		category: CategoryImages,
		match: func(b []byte) bool {
			return len(b) >= 8 && bytes.HasPrefix(b, []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a})
		},
	},
	// JPEG (\xff\xd8\xff)
	{
		category: CategoryImages,
		match: func(b []byte) bool {
			return len(b) >= 3 && bytes.HasPrefix(b, []byte{0xff, 0xd8, 0xff})
		},
	},
	// GIF (GIF87a or GIF89a)
	{
		category: CategoryImages,
		match: func(b []byte) bool {
			return len(b) >= 6 && (bytes.HasPrefix(b, []byte("GIF87a")) || bytes.HasPrefix(b, []byte("GIF89a")))
		},
	},
	// ZIP (PK\x03\x04)
	{
		category: CategoryArchives,
		match: func(b []byte) bool {
			return len(b) >= 4 && bytes.HasPrefix(b, []byte{0x50, 0x4b, 0x03, 0x04})
		},
	},
	// 7-Zip (7z\xbc\xaf\x27\x1c)
	{
		category: CategoryArchives,
		match: func(b []byte) bool {
			return len(b) >= 6 && bytes.HasPrefix(b, []byte{0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c})
		},
	},
	// RAR (Rar!\x1a\x07)
	{
		category: CategoryArchives,
		match: func(b []byte) bool {
			return len(b) >= 7 && bytes.HasPrefix(b, []byte{0x52, 0x61, 0x72, 0x21, 0x1a, 0x07})
		},
	},
	// GZIP (\x1f\x8b)
	{
		category: CategoryArchives,
		match: func(b []byte) bool {
			return len(b) >= 2 && bytes.HasPrefix(b, []byte{0x1f, 0x8b})
		},
	},
	// BZIP2 (BZh)
	{
		category: CategoryArchives,
		match: func(b []byte) bool {
			return len(b) >= 3 && bytes.HasPrefix(b, []byte{0x42, 0x5a, 0x68})
		},
	},
	// XZ (\xfd7zXZ\x00)
	{
		category: CategoryArchives,
		match: func(b []byte) bool {
			return len(b) >= 6 && bytes.HasPrefix(b, []byte{0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00})
		},
	},
	// ELF Executable (\x7fELF)
	{
		category: CategorySoftware,
		match: func(b []byte) bool {
			return len(b) >= 4 && bytes.HasPrefix(b, []byte{0x7f, 0x45, 0x4c, 0x46})
		},
	},
	// Windows PE / EXE (MZ)
	{
		category: CategorySoftware,
		match: func(b []byte) bool {
			return len(b) >= 2 && bytes.HasPrefix(b, []byte{0x4d, 0x5a})
		},
	},
	// MP4 Video (ftyp box at byte offset 4-7)
	{
		category: CategoryVideos,
		match: func(b []byte) bool {
			return len(b) >= 12 && bytes.Equal(b[4:8], []byte("ftyp"))
		},
	},
	// MKV / WebM (\x1a\x45\xdf\xa3)
	{
		category: CategoryVideos,
		match: func(b []byte) bool {
			return len(b) >= 4 && bytes.HasPrefix(b, []byte{0x1a, 0x45, 0xdf, 0xa3})
		},
	},
	// MP3 with ID3 tag
	{
		category: CategoryAudio,
		match: func(b []byte) bool {
			return len(b) >= 3 && bytes.HasPrefix(b, []byte("ID3"))
		},
	},
	// FLAC (fLaC)
	{
		category: CategoryAudio,
		match: func(b []byte) bool {
			return len(b) >= 4 && bytes.HasPrefix(b, []byte("fLaC"))
		},
	},
	// OGG Audio (OggS)
	{
		category: CategoryAudio,
		match: func(b []byte) bool {
			return len(b) >= 4 && bytes.HasPrefix(b, []byte("OggS"))
		},
	},
}

var extensionCategories = map[string]Category{
	// Documents
	".pdf":   CategoryDocuments,
	".doc":   CategoryDocuments,
	".docx":  CategoryDocuments,
	".xls":   CategoryDocuments,
	".xlsx":  CategoryDocuments,
	".ppt":   CategoryDocuments,
	".pptx":  CategoryDocuments,
	".txt":   CategoryDocuments,
	".rtf":   CategoryDocuments,
	".odt":   CategoryDocuments,
	".epub":  CategoryDocuments,
	".md":    CategoryDocuments,
	".csv":   CategoryDocuments,
	// Videos
	".mp4":  CategoryVideos,
	".mkv":  CategoryVideos,
	".avi":  CategoryVideos,
	".mov":  CategoryVideos,
	".wmv":  CategoryVideos,
	".flv":  CategoryVideos,
	".webm": CategoryVideos,
	".m4v":  CategoryVideos,
	// Audio
	".mp3":  CategoryAudio,
	".flac": CategoryAudio,
	".wav":  CategoryAudio,
	".aac":  CategoryAudio,
	".ogg":  CategoryAudio,
	".m4a":  CategoryAudio,
	".opus": CategoryAudio,
	// Archives
	".zip":    CategoryArchives,
	".tar":    CategoryArchives,
	".gz":     CategoryArchives,
	".tar.gz": CategoryArchives,
	".tgz":    CategoryArchives,
	".bz2":    CategoryArchives,
	".xz":     CategoryArchives,
	".7z":     CategoryArchives,
	".rar":    CategoryArchives,
	".iso":    CategoryArchives,
	// Software
	".exe":      CategorySoftware,
	".msi":      CategorySoftware,
	".dmg":      CategorySoftware,
	".pkg":      CategorySoftware,
	".deb":      CategorySoftware,
	".rpm":      CategorySoftware,
	".appimage": CategorySoftware,
	".apk":      CategorySoftware,
	// Images
	".png":  CategoryImages,
	".jpg":  CategoryImages,
	".jpeg": CategoryImages,
	".gif":  CategoryImages,
	".webp": CategoryImages,
	".bmp":  CategoryImages,
	".svg":  CategoryImages,
	".ico":  CategoryImages,
}

// MatchExtension inspects file extension (including multi-part like .tar.gz)
// and returns the mapped Category or CategoryMiscellaneous.
func MatchExtension(filename string) Category {
	lower := strings.ToLower(filename)
	if strings.HasSuffix(lower, ".tar.gz") {
		return CategoryArchives
	}

	ext := strings.ToLower(filepath.Ext(filename))
	if cat, ok := extensionCategories[ext]; ok {
		return cat
	}

	return CategoryMiscellaneous
}
