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
   python -m grpc_tools.protoc -I. --python_out=generated --pyi_out=generated --grpc_python_out=generated cloudberry_storage.proto
   ```