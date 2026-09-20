# Local Embedding Models

This directory is the default local model search path for BicDB development.
Large model payloads are intentionally ignored by git; keep them on disk or
manage them with a release artifact/LFS workflow.

The current local runtime expects:

```text
models/embeddinggemma-300m-ONNX/config.json
models/embeddinggemma-300m-ONNX/tokenizer_config.json
models/embeddinggemma-300m-ONNX/tokenizer.json
models/embeddinggemma-300m-ONNX/onnx/model_q4.onnx
models/embeddinggemma-300m-ONNX/onnx/model_q4.onnx_data
```

Register the local files with:

```bash
bicdb model enable /path/to/db embeddinggemma-300m
```
