import paramiko
import time

HOSTNAME_IP = [""]
PORT = 22
USERNAME = ""
PASSWORD = ""
KAFKA_SERVICE = "kafka"        # Kafka servisi ismi (systemctl)
LOG_FILE = "/kafka/broker/kafka_2.13-4.0.0/logs/server.log"  # Kafka log dosyası
CHECK_INTERVAL = 60 


def check_kafka_status(ssh_client):
    stdin, stdout, stderr = ssh_client.exec_command(f"systemctl is-active {KAFKA_SERVICE}")
    status = stdout.read().decode().strip()
    return status

def get_recent_errors(ssh_client, lines=10):
    stdin, stdout, stderr = ssh_client.exec_command(f"tail -n {lines} {LOG_FILE} | grep ERROR")
    errors = stdout.read().decode().strip()
    return errors if errors else "Son ERROR bulunamadı."


def conn():
    for ip in HOSTNAME_IP:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            ssh.connect(ip, PORT, USERNAME, PASSWORD)
            while True:
                print("\n---- Kafka Health Check ----")
                
                status = check_kafka_status(ssh)
                print(f"Kafka Servis Durumu: {status}")
                
                errors = get_recent_errors(ssh)
                print(f"Son Log ERROR’ları:\n{errors}")
                
                print(f"\nBir sonraki kontrol {CHECK_INTERVAL} saniye sonra...\n")
                time.sleep(CHECK_INTERVAL)
        
        except Exception as e:
            print(f"Connection to {ip} failed: {e}")
        finally:
            ssh.close()


if __name__ == "__main__":
    conn()

# SSH client oluştur
# ssh = paramiko.SSHClient()
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# try:
#     # Sunucuya bağlai
#     ssh.connect(hostname, port, username, password)

#     # Komut çalıştır
#     stdin, stdout, stderr = ssh.exec_command("docker --version")
#     print(stdout.read().decode())

# finally:
#     # Bağlantıyı kapat
#     ssh.close()
