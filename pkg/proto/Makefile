all: clean proto

clean:
	rm -rf datasource/*.pb.go
	rm -rf renderer/*.pb.go

proto:
	protoc -I . datasource.proto --go_out=plugins=grpc:datasource
	protoc -I . renderer.proto --go_out=plugins=grpc:renderer