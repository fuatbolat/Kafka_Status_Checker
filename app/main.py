import paramiko
import time
import yagmail



HOSTNAME_IP = [""]
PORT = 22
USERNAME = ""
PASSWORD = ""
KAFKA_SERVICE = "kafka"        
LOG_FILE = "/kafka/broker/kafka_2.13-4.0.0/logs/server.log"  
CHECK_INTERVAL = 120 
EMAIL_USER = ""        
EMAIL_PASS = ""           
TO_EMAIL = ""         

def send_email(subject, body):
    yag = yagmail.SMTP(EMAIL_USER, EMAIL_PASS)
    yag.send(
        to=TO_EMAIL,
        subject=subject,
        contents=body
    )
# yag = yagmail.SMTP(EMAIL_USER, EMAIL_PASS)
# subject = f"Kafka Servis Uyarısı - {KAFKA_SERVICE}"
# body = f"Uyarı: Kafka servisi {KAFKA_SERVICE} durumu '{status}' olarak tespit edildi. Lütfen kontrol ediniz."
# yag.send(TO_EMAIL, subject, body)
# print(f"Uyarı maili gönderildi: {TO_EMAIL}")



def check_kafka_status(ssh_client):
    stdin, stdout, stderr = ssh_client.exec_command(f"systemctl is-active {KAFKA_SERVICE}")
    status = stdout.read().decode().strip()
    # burası tam tersi oalrak değiştir !=
    if status == "active":
        subject = "Kafka Durumu: Sağlıklı"
        body = f"Kafka servisi çalışıyor.\nDurum: {status}"
        send_email(subject, body)
    else:
        subject = "Kafka Durumu: Sorunlu"
        errors = get_recent_errors(ssh_client)
        body = f"Kafka servisi çalışmıyor veya kurulu değil!\nDurum: {status}\n\nSon Hatalar:\n{errors}"
        send_email(subject, body)
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
            ssh.connect(ip,PORT,USERNAME,PASSWORD)
            while True:
                print("\n---- Kafka Health Check ----")
                
                #status = check_kafka_status(ssh)
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
  

