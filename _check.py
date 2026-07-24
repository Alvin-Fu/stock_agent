from storage.sqlite.stock_storage import get_db
db = get_db()

triggers = db.get_active_industry_triggers("半导体")
print(f"【行业触发条件】共{len(triggers)}条活跃")
for t in triggers:
    tag = "V" if t["trigger_type"]=="valuation" else "N"
    pc = f" PE={t['pe_percentile_below']}" if t.get("pe_percentile_below") else ""
    print(f"  {tag} #{t['id']} {t['description'][:80]}{pc}")

codes = {"002371":"北方华创","688361":"中科飞测","002156":"通富微电","600584":"长电科技"}
print("\n【个股反馈】")
for code, name in codes.items():
    fb = db.get_feedback_for_target(code=code)
    print(f"  {name}: {len(fb) if fb else 0}条")

print("\n【行业反馈】")
fb = db.get_feedback_for_target(target_name="半导体%")
for f in fb:
    print(f"  {f['target_name']}: {f['content'][:70]}")

print("\n【通用反馈】")
fb = db.get_feedback_for_target(target_name="通用%")
for f in fb:
    print(f"  {f['target_name']}: {f['content'][:70]}")
