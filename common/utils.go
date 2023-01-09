package common

import (
	"encoding/base64"
)

func GenResolverTag(exporterTag string) string {
	return "resolver-" + exporterTag
}

func Base64StdDecode(src []byte) ([]byte, error) {
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(src)))
	n, err := base64.StdEncoding.Decode(dst, src)
	if err != nil {
		return nil, err
	}
	dst = dst[:n]
	return dst, nil
}
