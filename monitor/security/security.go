package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// Claims 是 SHore 使用的 JWT 载荷。
type Claims struct {
	UserID   uint   `json:"userId"`
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// Encryptor 负责敏感字段的对称加解密。
type Encryptor struct {
	aead cipher.AEAD
}

// NewEncryptor 创建 AES-GCM 加密器。
func NewEncryptor(secret string) *Encryptor {
	sum := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		panic(err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}

	return &Encryptor{aead: aead}
}

// Encrypt 对字符串做 AES-GCM 加密。
func (e *Encryptor) Encrypt(plain string) (string, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	sealed := e.aead.Seal(nonce, nonce, []byte(plain), nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt 对密文做解密。
func (e *Encryptor) Decrypt(cipherText string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(cipherText)
	if err != nil {
		return "", err
	}

	nonceSize := e.aead.NonceSize()
	if len(raw) < nonceSize {
		return "", errors.New("cipher text too short")
	}

	nonce := raw[:nonceSize]
	body := raw[nonceSize:]
	plain, err := e.aead.Open(nil, nonce, body, nil)
	if err != nil {
		return "", err
	}

	return string(plain), nil
}

// HashPassword 将明文密码转换为 Bcrypt 哈希。
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}

	return string(hash), nil
}

// ComparePassword 校验密码哈希。
func ComparePassword(hash string, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

// IssueToken 生成带 24 小时有效期的 JWT。
func IssueToken(secret string, userID uint, username string) (string, error) {
	claims := Claims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Subject:   username,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// ParseToken 校验并解析 JWT。
func ParseToken(secret string, tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (any, error) {
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token")
	}

	return claims, nil
}
