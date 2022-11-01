package test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetMongoUri() string {
	env := os.Getenv("MONGO_URI")
	if env != "" {
		return env
	}
	return "mongodb://127.0.0.1:27017"
}

func StartMongoDB() (cleanup func()) {
	_, currentFilePath, _, _ := runtime.Caller(0)
	dirpath := path.Dir(currentFilePath)

	makeDown := func() {
		cmd := exec.Command(
			"docker-compose",
			"-f",
			fmt.Sprintf("%s/docker-compose.yml", dirpath),
			"down",
			"-v",
			"--remove-orphans",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatalf("docker-compose down: %s", err)
		}
	}
	makeDown()

	cmd := exec.Command(
		"docker-compose",
		"-f",
		fmt.Sprintf("%s/docker-compose.yml", dirpath),
		"build",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("docker-compose build: %s", err)
	}
	cmd = exec.Command(
		"docker-compose",
		"-f",
		fmt.Sprintf("%s/docker-compose.yml", dirpath),
		"up",
		"-V",
	)
	//cmd.Stdout = os.Stdout
	//cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("running docker-compose: %s", err)
	}

	limit := 10
	retries := 0
	var err error
	for retries < limit {
		err = checkServices()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
		retries++
	}
	if retries == limit {
		makeDown()
		if err != nil {
			log.Fatalf("connecting to MongoDB: %s", err)
		}
		log.Fatalf("max retries to connect with MongoDB")
	}
	return makeDown
}

func checkServices() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	mc, err := mongo.Connect(ctx, options.Client().ApplyURI(GetMongoUri()))
	if err != nil {
		return err
	}
	if err = mc.Ping(ctx, nil); err != nil {
		return err
	}
	return nil
}
