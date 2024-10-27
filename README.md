1. Install all in `requirements.txt`
2.  On Ubuntu/Debian
    ```commandline
    sudo apt install protobuf-compiler
    ```
    On macOS
    ```
    brew install protobuf
    ```
3. Generate spec
   ```commandline
   python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. cloudberry_storage.proto
   ```