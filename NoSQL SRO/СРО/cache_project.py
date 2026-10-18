"""СРО, вариант 20: платформа онлайн-обучения. MongoDB (данные) + Redis (кэш).
Жолдас Қайсар ИС 24-22"""
import json
import os
import sys
import time

import redis
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
try:
    db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)["learning"]
    db.command("ping")
except Exception as e:
    sys.exit(f"Не удалось подключиться к MongoDB ({MONGO_URI}): {e}\nЗапустите MongoDB, см. README.md")
try:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
except Exception as e:
    sys.exit(f"Не удалось подключиться к Redis ({REDIS_URL}): {e}\nЗапустите Redis, см. README.md")
TTL = 300  # секунд жизни карточки курса в кэше


def seed():
    """Создаёт тестовые данные: 10 пользователей, 5 курсов, 24 записи (всего 39)."""
    for name in ("users", "courses", "enrollments"):
        db[name].drop()
    for pattern in ("course:*", "popular"):
        for key in r.scan_iter(pattern):
            r.delete(key)
    db.users.insert_many(
        [{"_id": "u1", "name": "Айдар", "role": "teacher"}, {"_id": "u2", "name": "Мадина", "role": "teacher"}]
        + [{"_id": f"s{i}", "name": f"Студент {i}", "role": "student", "email": f"s{i}@example.com"}
           for i in range(1, 9)])
    courses = [("Python", "Programming", "beginner", 9900, "u1"), ("NoSQL", "Databases", "intermediate", 19900, "u1"),
               ("MongoDB", "Databases", "beginner", 14900, "u2"), ("FastAPI", "Web", "intermediate", 14900, "u2"),
               ("ML", "Data Science", "advanced", 29900, "u1")]
    db.courses.insert_many([
        {"_id": f"c{i}", "title": t, "category": cat, "level": lv, "price": p, "teacher_id": tid,
         "tags": [cat.lower()], "modules": [{"title": "Введение", "lessons": 3}]}
        for i, (t, cat, lv, p, tid) in enumerate(courses, 1)])
    db.enrollments.create_index([("user_id", 1), ("course_id", 1)], unique=True)  # защита от дубля
    for i in range(1, 9):
        for j in range(3):
            enroll(f"s{i}", f"c{(i + j) % 5 + 1}", progress=100 if (i + j) % 4 == 0 else (i * 17 + j * 40) % 100)


def get_course(course_id):
    """Cache-aside: сначала Redis, при промахе MongoDB + запись в кэш с TTL."""
    key = f"course:{course_id}"
    cached = r.get(key)
    if cached:
        return json.loads(cached), "cache"
    doc = db.courses.find_one({"_id": course_id})
    if doc:
        r.setex(key, TTL, json.dumps(doc, ensure_ascii=False))
    return doc, "mongo"


def update_price(course_id, price):
    """Изменение цены: обновляем MongoDB и удаляем ключ кэша (инвалидирование)."""
    db.courses.update_one({"_id": course_id}, {"$set": {"price": price}})
    r.delete(f"course:{course_id}")


def enroll(user_id, course_id, progress=0):
    """Запись на курс со snapshot цены; повторная запись запрещена уникальным индексом."""
    course = db.courses.find_one({"_id": course_id})
    try:
        db.enrollments.insert_one({
            "user_id": user_id, "course_id": course_id, "progress": progress,
            "status": "completed" if progress == 100 else "active",
            "course_snapshot": {"title": course["title"], "price": course["price"]}})
    except DuplicateKeyError:
        return False
    r.zincrby("popular", 1, course_id)  # рейтинг популярности в Redis Sorted Set
    return True


def delete_enrollment(user_id, course_id):
    """Удаление записи на курс; рейтинг в Redis уменьшается."""
    deleted = db.enrollments.delete_one({"user_id": user_id, "course_id": course_id}).deleted_count
    if deleted:
        r.zincrby("popular", -1, course_id)
    return bool(deleted)


def delete_course(course_id):
    """Удаление курса: чистим кэш и рейтинг. Записи остаются: в них есть snapshot курса."""
    db.courses.delete_one({"_id": course_id})
    r.delete(f"course:{course_id}")
    r.zrem("popular", course_id)


def top_popular(n=3):
    """Топ курсов из Redis ZSET (ZREVRANGE)."""
    return [(c, int(s)) for c, s in r.zrevrange("popular", 0, n - 1, withscores=True)]


def show(title, rows):
    print(f"\n{title}")
    for row in rows:
        print("  ", row)


def queries():
    """8 демонстрационных запросов; Q5-Q7 нетривиальные (aggregation pipeline)."""
    show("Q1. Курсы категории Databases по цене:",
         db.courses.find({"category": "Databases"}, {"title": 1, "price": 1}).sort("price", 1))
    show("Q2. Курсы с ценой 10000-20000 и тегом databases/web ($gte, $lte, $in):",
         db.courses.find({"price": {"$gte": 10000, "$lte": 20000}, "tags": {"$in": ["databases", "web"]}},
                         {"title": 1, "price": 1}).sort([("price", 1), ("_id", 1)]))
    show("Q3. Курсы студента s1 с прогрессом и snapshot:",
         db.enrollments.find({"user_id": "s1"}, {"_id": 0, "course_id": 1, "progress": 1, "status": 1,
                                                 "course_snapshot": 1}).sort("progress", -1))
    show("Q4. Средняя цена по категориям ($group):",
         db.courses.aggregate([{"$group": {"_id": "$category", "avg_price": {"$avg": "$price"}}},
                               {"$sort": {"avg_price": -1, "_id": 1}}]))
    show("Q5. Топ-3 курса по записям ($group + $lookup + $unwind):", db.enrollments.aggregate([
        {"$group": {"_id": "$course_id", "n": {"$sum": 1}, "avg_progress": {"$avg": "$progress"}}},
        {"$sort": {"n": -1, "_id": 1}}, {"$limit": 3},
        {"$lookup": {"from": "courses", "localField": "_id", "foreignField": "_id", "as": "c"}},
        {"$unwind": "$c"},
        {"$project": {"_id": 0, "title": "$c.title", "enrollments": "$n", "avg_progress": 1}}]))
    show("Q6. Завершили курс по категориям ($lookup + $group + $cond):", db.enrollments.aggregate([
        {"$lookup": {"from": "courses", "localField": "course_id", "foreignField": "_id", "as": "c"}},
        {"$unwind": "$c"},
        {"$group": {"_id": "$c.category", "total": {"$sum": 1},
                    "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}}}},
        {"$sort": {"_id": 1}}]))
    show("Q7. Нагрузка преподавателей: уникальные студенты (двойной $group + $lookup):", db.enrollments.aggregate([
        {"$lookup": {"from": "courses", "localField": "course_id", "foreignField": "_id", "as": "c"}},
        {"$unwind": "$c"},
        {"$group": {"_id": {"t": "$c.teacher_id", "u": "$user_id"}, "n": {"$sum": 1}}},
        {"$group": {"_id": "$_id.t", "unique_students": {"$sum": 1}, "enrollments": {"$sum": "$n"}}},
        {"$sort": {"unique_students": -1, "_id": 1}},
        {"$lookup": {"from": "users", "localField": "_id", "foreignField": "_id", "as": "u"}},
        {"$unwind": "$u"},
        {"$project": {"_id": 0, "teacher": "$u.name", "unique_students": 1, "enrollments": 1}}]))
    show("Q8. Топ-3 популярных курса из Redis (ZREVRANGE):", top_popular(3))


def tests():
    """4 тестовых сценария (assert); при ошибке скрипт остановится."""
    print("\nТесты:")
    r.delete("course:c3")
    assert get_course("c3")[1] == "mongo" and get_course("c3")[1] == "cache" and 0 < r.ttl("course:c3") <= TTL
    print("  Тест 1 (cache-aside: промах, попадание, TTL): OK")

    update_price("c3", 15900)
    assert r.get("course:c3") is None
    doc, src = get_course("c3")
    assert src == "mongo" and doc["price"] == 15900
    print("  Тест 2 (инвалидирование кэша после смены цены): OK")

    assert enroll("t1", "c4") is True and enroll("t1", "c4") is False
    price = db.courses.find_one({"_id": "c4"})["price"]
    update_price("c4", price + 1000)
    assert db.enrollments.find_one({"user_id": "t1", "course_id": "c4"})["course_snapshot"]["price"] == price
    assert delete_enrollment("t1", "c4") is True
    print("  Тест 3 (запрет дубля, snapshot цены, удаление записи): OK")

    db.courses.insert_one({"_id": "c_tmp", "title": "Temp", "category": "Web", "level": "beginner",
                           "price": 1, "teacher_id": "u1", "tags": []})
    get_course("c_tmp")
    delete_course("c_tmp")
    assert r.get("course:c_tmp") is None and db.courses.find_one({"_id": "c_tmp"}) is None
    print("  Тест 4 (удаление курса чистит кэш): OK")


def timing(n=1000):
    t = time.perf_counter()
    for _ in range(n):
        db.courses.find_one({"_id": "c1"})
    mongo_ms = (time.perf_counter() - t) * 1000
    get_course("c1")
    t = time.perf_counter()
    for _ in range(n):
        get_course("c1")
    cache_ms = (time.perf_counter() - t) * 1000
    print(f"\n{n} чтений карточки: MongoDB {mongo_ms:.1f} мс, через Redis {cache_ms:.1f} мс")


if __name__ == "__main__":
    seed()
    print("1) Первое чтение:", get_course("c1")[1], "| повторное:", get_course("c1")[1])
    update_price("c1", 12345)
    doc, src = get_course("c1")
    print("2) После смены цены:", doc["price"], "| источник:", src, "(кэш сброшен)")
    print("3) Повторная запись s1 на c2 отклонена:", enroll("s1", "c2") is False)
    queries()
    tests()
    timing()
