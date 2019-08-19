all: npm_install npm_prod clean build copy

npm_install:
	npm install

npm_prod:
	npm run build:prod

clean:
	rm -rf ./dist/*_linux_amd64
	rm -rf ./dist/*_darwin_amd64
	rm -rf ./dist/*_windows_amd64.exe

build:
	GOOS=linux GOARCH=amd64 go build -o clickhouse-datasource-plugin_linux_amd64 ./pkg
	GOOS=darwin GOARCH=amd64 go build -o clickhouse-datasource-plugin_darwin_amd64 ./pkg
	GOOS=windows GOARCH=amd64 go build -o clickhouse-datasource-plugin_windows_amd64.exe ./pkg

copy:
	mv clickhouse-datasource-plugin_linux_amd64 ./dist/
	mv clickhouse-datasource-plugin_darwin_amd64 ./dist/
	mv clickhouse-datasource-plugin_windows_amd64.exe ./dist/
