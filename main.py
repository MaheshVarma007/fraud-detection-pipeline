import subprocess
import time
import threading
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


def run_process(cmd, name):
    """Run a subprocess command in a separate thread."""
    def target():
        print(f"[START] {name}")
        proc = subprocess.Popen(cmd, shell=True, cwd=PROJECT_ROOT)
        proc.wait()
        print(f"[END] {name}")
    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t

def main():
    """Main orchestration function for the pipeline."""
    # print("[INFO] Starting Kafka and ZooKeeper with Docker Compose...")
    # subprocess.run("docker-compose up -d", shell=True, check=True, cwd=PROJECT_ROOT)
    # time.sleep(10)

    print("[INFO] Creating Kafka topic...")
    subprocess.run("python3 -m fd_kafka.create_payments_topic", shell=True, check=True, cwd=PROJECT_ROOT)

    print("[INFO] Generating payment events...")
    subprocess.run("python3 -m avro.generate_events", shell=True, check=True, cwd=PROJECT_ROOT)

    print("[INFO] Starting Alert Dispatcher (FastAPI)...")
    dispatcher_thread = run_process("python3 -m uvicorn fd_kafka.alert_dispatcher:app --reload", "Alert Dispatcher")
    time.sleep(5)

    print("[INFO] Starting Async Consumer...")
    consumer_thread = run_process("python3 -m async_consumer", "Async Consumer")
    time.sleep(2)

    print("[INFO] Starting Async Producer...")
    producer_thread = run_process("python3 -m async_producer", "Async Producer")

    producer_thread.join()
    print("[INFO] Producer finished. You can stop the other services with Ctrl+C if desired.")

if __name__ == "__main__":
    main()