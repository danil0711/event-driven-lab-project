import subprocess
import time


def run(cmd):
    subprocess.run(cmd, check=True)


def get_kafka_health():
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format={{.State.Health.Status}}",
            "ed-lab-project-kafka-broker",
        ],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def main():
    print("Starting infrastructure...")

    run(["docker", "compose", "-f", "docker-compose.infra.yml", "up", "-d"])

    print("Waiting for Kafka...")

    while True:
        status = get_kafka_health()
        print("Kafka status:", status)

        if status == "healthy":
            break

        time.sleep(3)

    print("Starting services...")

    run(
        [
            "docker",
            "compose",
            "-f",
            "docker-compose.services.yml",
            "up",
            "-d",
            "--build",
        ]
    )

    print("Done")


if __name__ == "__main__":
    main()
