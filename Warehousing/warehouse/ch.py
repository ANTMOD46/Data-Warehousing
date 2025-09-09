from clickhouse_driver import Client

CH_HOST = "localhost"
CH_PORT = 9000  # ClickHouse port
CH_USER = "default"  # ชื่อผู้ใช้ (ถ้ามี)
CH_PASSWORD = ""  # รหัสผ่าน (ถ้ามี)
CH_DB = "market"  # ชื่อฐานข้อมูลที่ต้องการใช้งาน

def ch_client():
    return Client(
        host=CH_HOST, port=CH_PORT,
        user=CH_USER, password=CH_PASSWORD,
        database=CH_DB,
        settings={"use_numpy": False},
    )
