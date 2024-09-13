package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	// Carrega as variáveis de ambiente do .env
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Erro ao carregar o arquivo .env: %v", err)
	}

	// Configurações do MinIO a partir do .env
	endpoint := os.Getenv("MINIO_ENDPOINT")
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	secretAccessKey := os.Getenv("MINIO_SECRET_KEY")
	useSSL := os.Getenv("MINIO_USE_SSL") == "true"

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalf("Erro ao criar cliente MinIO: %v", err)
	}

	localDir := os.Getenv("LOCAL_DIR")      // Diretório local com arquivos para sincronizar
	bucketName := os.Getenv("MINIO_BUCKET") // Nome do bucket MinIO

	exists, err := client.BucketExists(context.Background(), bucketName)
	if err != nil {
		log.Fatalf("Erro ao verificar existência do bucket: %v", err)
	}
	if !exists {
		log.Fatalf("Bucket %s não existe", bucketName)
	}

	err = filepath.WalkDir(localDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			fileInfo, err := file.Stat()
			if err != nil {
				return err
			}

			objectName := filepath.Base(path)

			_, err = client.PutObject(context.Background(), bucketName, objectName, file, fileInfo.Size(), minio.PutObjectOptions{})
			if err != nil {
				log.Printf("Erro ao enviar o arquivo %s: %v", objectName, err)
				saveStatus(objectName, "falhou")
				return err
			}

			saveStatus(objectName, "sucesso")
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Erro ao sincronizar arquivos: %v", err)
	}

	fmt.Println("Sincronização concluída com sucesso")
}

func saveStatus(fileName, status string) {
	statusFile, err := os.OpenFile("sync_status.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Erro ao abrir arquivo de status: %v", err)
		return
	}
	defer statusFile.Close()

	_, err = fmt.Fprintf(statusFile, "Arquivo: %s, Status: %s\n", fileName, status)
	if err != nil {
		log.Printf("Erro ao escrever status no arquivo: %v", err)
	}
}
