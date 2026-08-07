// Package credentials — локальное хранение dev-токена после qrok login.
package credentials

import (
	"encoding/json"
	"os"
	"path/filepath"

	"qrok/pkg/fault"
)

// File — содержимое ~/.config/qrok/credentials.json (или путь из QROK_CREDENTIALS_FILE).
type File struct {
	APIURL      string `json:"api_url"`
	Gateway     string `json:"gateway,omitempty"`
	AccessToken string `json:"access_token"`
}

// DefaultPath возвращает путь к файлу credentials.
func DefaultPath() (string, error) {
	if p := os.Getenv("QROK_CREDENTIALS_FILE"); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fault.ErrInternal.Wrap(err, "failed to resolve home directory").WithOp("credentials.path")
	}
	return filepath.Join(home, ".config", "qrok", "credentials.json"), nil
}

// Load читает credentials; если файла нет — (nil, nil).
func Load() (*File, error) {
	path, err := DefaultPath()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fault.ErrInternal.Wrap(err, "failed to read credentials").WithOp("credentials.load")
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fault.ErrInternal.Wrap(err, "corrupt credentials file").WithOp("credentials.load")
	}
	return &f, nil
}

// Save записывает credentials с правами 0600.
func Save(f *File) error {
	if f == nil || f.AccessToken == "" {
		return fault.ErrValidation.New("empty access_token").WithOp("credentials.save")
	}
	path, err := DefaultPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fault.ErrInternal.Wrap(err, "failed to create credentials directory").WithOp("credentials.save")
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fault.ErrInternal.Wrap(err, "failed to serialize credentials").WithOp("credentials.save")
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fault.ErrInternal.Wrap(err, "failed to write credentials").WithOp("credentials.save")
	}
	return nil
}

// ResolveToken: явный token из конфига → credentials → пусто.
func ResolveToken(configToken string) (string, error) {
	if configToken != "" {
		return configToken, nil
	}
	creds, err := Load()
	if err != nil {
		return "", err
	}
	if creds == nil || creds.AccessToken == "" {
		return "", fault.ErrUnauthorized.
			New("dev-токен не найден").
			WithOp("credentials.resolve").
			WithHint("run qrok login or set listen.token in config")
	}
	return creds.AccessToken, nil
}
