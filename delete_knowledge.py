import chromadb

# 1. 连接到你的远程 Chroma (Docker 部署)
client = chromadb.HttpClient(host='127.0.0.1', port=8000)

# 2. 定义你要删除的集合名称
collection_name = "collection_stock"



# 3. (可选) 立即创建一个干净的同名集合
# 注意：不需要手动指定维度，入库时第一个数据存入后会自动确定维度

if __name__ == "__main__":
    try:
        # 检查是否存在并删除
        client.delete_collection(name=collection_name)
        print(f"✅ 成功删除旧集合: {collection_name}，维度冲突已解除。")
    except Exception as e:
        print(f"⚠️ 删除失败或集合不存在: {e}")