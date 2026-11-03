package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

type CertificateManager struct {
	certFile string
	keyFile  string
}

func NewCertificateManager(certFile, keyFile string) *CertificateManager {
	return &CertificateManager{
		certFile: certFile,
		keyFile:  keyFile,
	}
}

func (cm *CertificateManager) EnsureDirectories() error {
	certDir := filepath.Dir(cm.certFile)
	if err := os.MkdirAll(certDir, 0755); err != nil {
		return fmt.Errorf("创建证书目录失败: %v", err)
	}
	
	keyDir := filepath.Dir(cm.keyFile)
	if err := os.MkdirAll(keyDir, 0755); err != nil {
		return fmt.Errorf("创建密钥目录失败: %v", err)
	}
	
	return nil
}

func (cm *CertificateManager) CertificateExists() bool {
	if _, err := os.Stat(cm.certFile); os.IsNotExist(err) {
		return false
	}
	if _, err := os.Stat(cm.keyFile); os.IsNotExist(err) {
		return false
	}
	return true
}

func (cm *CertificateManager) ValidateCertificate() bool {
	if !cm.CertificateExists() {
		return false
	}
	
	certPEM, err := os.ReadFile(cm.certFile)
	if err != nil {
		return false
	}
	
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}
	
	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return false
	}
	
	return true
}

type GenerateOptions struct {
	Organization  string
	Country       string
	Province      string
	Locality      string
	ServerIPs     []string
	ServerDomains []string
	ValidityDays  int
}

func (cm *CertificateManager) GenerateSelfSignedCert(opts GenerateOptions) error {
	if err := cm.EnsureDirectories(); err != nil {
		return err
	}
	
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("生成私钥失败: %v", err)
	}
	
	notBefore := time.Now()
	notAfter := notBefore.Add(time.Duration(opts.ValidityDays) * 24 * time.Hour)
	
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return fmt.Errorf("生成序列号失败: %v", err)
	}
	
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{opts.Organization},
			Country:      []string{opts.Country},
			Province:     []string{opts.Province},
			Locality:     []string{opts.Locality},
			CommonName:   "lxcapi Self-Signed Certificate",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	
	for _, ip := range opts.ServerIPs {
		if parsedIP := net.ParseIP(ip); parsedIP != nil {
			template.IPAddresses = append(template.IPAddresses, parsedIP)
		}
	}
	
	for _, domain := range opts.ServerDomains {
		template.DNSNames = append(template.DNSNames, domain)
	}
	
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("创建证书失败: %v", err)
	}
	
	certOut, err := os.Create(cm.certFile)
	if err != nil {
		return fmt.Errorf("创建证书文件失败: %v", err)
	}
	defer certOut.Close()
	
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return fmt.Errorf("编码证书失败: %v", err)
	}
	
	keyOut, err := os.OpenFile(cm.keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("创建私钥文件失败: %v", err)
	}
	defer keyOut.Close()
	
	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("编码私钥失败: %v", err)
	}
	
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		return fmt.Errorf("写入私钥失败: %v", err)
	}
	
	return nil
}

