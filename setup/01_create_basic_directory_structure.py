import os

base_path = "/Volumes/main/financial/lakehouse"

folders = {
    "bronze": ["csv", "parquet"],
    "silver": [],
    "gold": []
}

for layer, subdirs in folders.items():
    layer_path = os.path.join(base_path, layer)
    os.makedirs(layer_path, exist_ok=True)
    print(f"📁 Layer created: {layer_path}")
    
    for subdir in subdirs:
        path = os.path.join(layer_path, subdir)
        os.makedirs(path, exist_ok=True)
        print(f"   ✅ Subdirectory created: {path}")

