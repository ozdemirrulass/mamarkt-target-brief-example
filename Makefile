build:
	env GOOS=linux GOARCH=amd64 go build -o ./build/main ./cmd
zip:
	zip -j ./build/main.zip ./build/main                      

.PHONY:	zip	build