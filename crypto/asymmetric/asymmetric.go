package asymmetric

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"math/big"

	"github.com/libp2p/go-libp2p/core/crypto"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
)

const (
	// NonceBytes is the length of nacl nonce.
	NonceBytes = 24

	// EphemeralPublicKeyBytes is the length of nacl ephemeral public key.
	EphemeralPublicKeyBytes = 32
)

var (
	// Nacl box decryption failed.
	BoxDecryptionError = errors.New("failed to decrypt curve25519")
	keyTypeError       = errors.New("could not determine key type")
	cipherTextError    = errors.New("malformed cipher text")
)

// EncryptionKey is a public key wrapper that can perform encryption.
type EncryptionKey struct {
	pk crypto.PubKey
}

// FromPubKey returns a key by parsing k into a public key.
func FromPubKey(pk crypto.PubKey) (*EncryptionKey, error) {
	if _, ok := pk.(*crypto.Ed25519PublicKey); !ok {
		return nil, keyTypeError
	}
	return &EncryptionKey{pk: pk}, nil
}

// Encrypt bytes with a public key.
func (k *EncryptionKey) Encrypt(plaintext []byte) ([]byte, error) {
	return encrypt(plaintext, k.pk)
}

// MarshalBinary implements BinaryMarshaler.
func (k *EncryptionKey) MarshalBinary() ([]byte, error) {
	return crypto.MarshalPublicKey(k.pk)
}

// DecryptionKey is a private key wrapper that can perform decryption.
type DecryptionKey struct {
	sk crypto.PrivKey
}

// FromPrivKey returns a key by parsing k into a private key.
func FromPrivKey(sk crypto.PrivKey) (*DecryptionKey, error) {
	if _, ok := sk.(*crypto.Ed25519PrivateKey); !ok {
		return nil, keyTypeError
	}
	return &DecryptionKey{sk: sk}, nil
}

// Encrypt bytes with a public key.
func (k *DecryptionKey) Encrypt(plaintext []byte) ([]byte, error) {
	return encrypt(plaintext, k.sk.GetPublic())
}

// Decrypt ciphertext with a private key.
func (k *DecryptionKey) Decrypt(ciphertext []byte) ([]byte, error) {
	return decrypt(ciphertext, k.sk)
}

// MarshalBinary implements BinaryMarshaler.
func (k *DecryptionKey) MarshalBinary() ([]byte, error) {
	return crypto.MarshalPrivateKey(k.sk)
}

func encrypt(plaintext []byte, pk crypto.PubKey) ([]byte, error) {
	ed25519Pubkey, ok := pk.(*crypto.Ed25519PublicKey)
	if ok {
		return encryptCurve25519(ed25519Pubkey, plaintext)
	}
	return nil, keyTypeError
}

func decrypt(ciphertext []byte, sk crypto.PrivKey) ([]byte, error) {
	ed25519Privkey, ok := sk.(*crypto.Ed25519PrivateKey)
	if ok {
		return decryptCurve25519(ed25519Privkey, ciphertext)
	}
	return nil, keyTypeError
}

func publicToCurve25519(k *crypto.Ed25519PublicKey) (*[EphemeralPublicKeyBytes]byte, error) {
	var cp [EphemeralPublicKeyBytes]byte
	var pk [EphemeralPublicKeyBytes]byte
	r, err := k.Raw()
	if err != nil {
		return nil, err
	}
	copy(pk[:], r)

	buf := ed25519PublicKeyToCurve25519(ed25519.PublicKey(pk[:]))
	copy(cp[:], buf)

	return &cp, nil
}

func encryptCurve25519(pubKey *crypto.Ed25519PublicKey, bytes []byte) ([]byte, error) {
	// generated ephemeral key pair
	ephemPub, ephemPriv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	// convert recipient's key into curve25519
	pk, err := publicToCurve25519(pubKey)
	if err != nil {
		return nil, err
	}

	// encrypt with nacl
	var ciphertext []byte
	var nonce [NonceBytes]byte
	n := make([]byte, NonceBytes)
	if _, err = rand.Read(n); err != nil {
		return nil, err
	}
	for i := 0; i < NonceBytes; i++ {
		nonce[i] = n[i]
	}
	ciphertext = box.Seal(ciphertext, bytes, &nonce, pk, ephemPriv)

	// prepend the ephemeral public key
	ciphertext = append(ephemPub[:], ciphertext...)

	// prepend nonce
	ciphertext = append(nonce[:], ciphertext...)
	return ciphertext, nil
}

func decryptCurve25519(privKey *crypto.Ed25519PrivateKey, ciphertext []byte) ([]byte, error) {
	curve25519Privkey, err := privateToCurve25519(privKey)
	if err != nil {
		return nil, err
	}

	var plaintext []byte

	if len(ciphertext) < NonceBytes+EphemeralPublicKeyBytes {
		return nil, cipherTextError
	}
	n := ciphertext[:NonceBytes]
	ephemPubkeyBytes := ciphertext[NonceBytes : NonceBytes+EphemeralPublicKeyBytes]
	ct := ciphertext[NonceBytes+EphemeralPublicKeyBytes:]

	var ephemPubkey [EphemeralPublicKeyBytes]byte
	for i := 0; i < EphemeralPublicKeyBytes; i++ {
		ephemPubkey[i] = ephemPubkeyBytes[i]
	}

	var nonce [NonceBytes]byte
	for i := 0; i < NonceBytes; i++ {
		nonce[i] = n[i]
	}

	plaintext, success := box.Open(plaintext, ct, &nonce, &ephemPubkey, curve25519Privkey)
	if !success {
		return nil, BoxDecryptionError
	}
	return plaintext, nil
}

func privateToCurve25519(k *crypto.Ed25519PrivateKey) (*[EphemeralPublicKeyBytes]byte, error) {
	var cs [EphemeralPublicKeyBytes]byte
	r, err := k.Raw()
	if err != nil {
		return nil, err
	}
	var sk [64]byte
	copy(sk[:], r)
	buf := ed25519PrivateKeyToCurve25519(ed25519.PrivateKey(sk[:]))
	copy(cs[:], buf)
	return &cs, nil
}

// The below code is copied exactly as found in:
// https://github.com/FiloSottile/age/blob/c9a35c072716b5ac6cd815366999c9e189b0c317/internal/agessh/agessh.go#L179
// Note: We should switch to x/crypto/ed25519 curve transformations when
// the following issue closes: https://github.com/golang/go/issues/20504.

// Copyright 2019 Google LLC
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
// * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
var curve25519P, _ = new(big.Int).SetString("57896044618658097711785492504343953926634992332820282019728792003956564819949", 10)

func ed25519PublicKeyToCurve25519(pk ed25519.PublicKey) []byte {
	// ed25519.PublicKey is a little endian representation of the y-coordinate,
	// with the most significant bit set based on the sign of the x-coordinate.
	bigEndianY := make([]byte, ed25519.PublicKeySize)
	for i, b := range pk {
		bigEndianY[ed25519.PublicKeySize-i-1] = b
	}
	bigEndianY[0] &= 0b0111_1111

	// The Montgomery u-coordinate is derived through the bilinear map
	//
	//     u = (1 + y) / (1 - y)
	//
	// See https://blog.filippo.io/using-ed25519-keys-for-encryption.
	y := new(big.Int).SetBytes(bigEndianY)
	denom := big.NewInt(1)
	denom.ModInverse(denom.Sub(denom, y), curve25519P) // 1 / (1 - y)
	u := y.Mul(y.Add(y, big.NewInt(1)), denom)
	u.Mod(u, curve25519P)

	out := make([]byte, curve25519.PointSize)
	uBytes := u.Bytes()
	for i, b := range uBytes {
		out[len(uBytes)-i-1] = b
	}

	return out
}
func ed25519PrivateKeyToCurve25519(pk ed25519.PrivateKey) []byte {
	h := sha512.New()
	h.Write(pk.Seed())
	out := h.Sum(nil)
	return out[:curve25519.ScalarSize]
}
