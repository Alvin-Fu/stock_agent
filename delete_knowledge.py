"""
删除远程 Chroma 中的知识库集合（用于解除向量维度冲突后重建）
连接信息与集合名统一从配置读取，避免删错集合。
"""
import chromadb

from utils.config import load_config

if __name__ == "__main__":
    cfg = load_config()
    chroma_cfg = cfg.get("chroma_server", {})
    collection_name = (
        cfg.get("knowledge_bases", {}).get("kb_stock", {}).get("collection_name", "collection_stock")
    )

    client = chromadb.HttpClient(
        host=chroma_cfg.get("host", "127.0.0.1"),
        port=chroma_cfg.get("port", 8000),
    )

    confirm = input(f"即将删除远程 Chroma 集合「{collection_name}」，输入 yes 确认: ").strip().lower()
    if confirm != "yes":
        print("已取消")
    else:
        try:
            client.delete_collection(name=collection_name)
            print(f"✅ 成功删除旧集合: {collection_name}，维度冲突已解除。")
        except Exception as e:
            print(f"⚠️ 删除失败或集合不存在: {e}")
