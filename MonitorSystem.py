from kazoo.client import KazooClient
from zookeeper_monitor import ZookeeperMonitor
from flink_monitor import FlinkMonitor
from utils.alert_utils import get_alert_system
import threading
import logging
from utils.zookeeper_utils import get_zk_client
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import smtplib
from email.mime.text import MIMEText
from alert_system import AlertSystem
def start_zookeeper_monitor():
    """
    启动 Zookeeper 监控。
    """
    zk_monitor = ZookeeperMonitor(zk_hosts="localhost:2181")
    zk_monitor.start()

def start_flink_monitor():
    """
    启动 Flink 监控。
    """
    flink_monitor = FlinkMonitor()
    flink_monitor.monitor_tasks()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # 启动 Zookeeper 监控
    zk_thread = threading.Thread(target=start_zookeeper_monitor)
    zk_thread.start()

    # 启动 Flink 监控
    flink_thread = threading.Thread(target=start_flink_monitor)
    flink_thread.start()

    zk_thread.join()
    flink_thread.join()

class ZookeeperMonitor:
    def __init__(self, zk_hosts, zk_path="/ispider"):
        """
        初始化 Zookeeper 监控器。
        :param zk_hosts: Zookeeper 服务器地址（如 "localhost:2181"）。
        :param zk_path: 监控的 Zookeeper 路径（默认 "/ispider"）。
        """
        self.zk_hosts = zk_hosts
        self.zk_path = zk_path
        self.zk = get_zk_client(zk_hosts)
        self.logger = logging.getLogger(__name__)

    def start(self):
        """
        启动 Zookeeper 监控。
        """
        self.zk.start()
        self.logger.info("Zookeeper connected successfully.")
        self.zk.ensure_path(self.zk_path)
        self.zk.ChildrenWatch(self.zk_path, self.watch_nodes)

    def watch_nodes(self, nodes):
        """
        监控节点变化。
        :param nodes: 当前节点列表。
        """
        self.logger.info(f"Current nodes: {nodes}")
        if len(nodes) == 0:
            self.logger.warning("No active nodes found!")
        else:
            self.logger.info(f"Active nodes: {nodes}")

    def stop(self):
        """
        停止 Zookeeper 监控。
        """
        self.zk.stop()
        self.logger.info("Zookeeper connection closed.")


def get_zk_client(zk_hosts):
    """
    获取 Zookeeper 客户端。
    :param zk_hosts: Zookeeper 服务器地址。
    :return: Zookeeper 客户端实例。
    """
    return KazooClient(hosts=zk_hosts)

class FlinkMonitor:
    def __init__(self):
        """
        初始化 Flink 监控器。
        """
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        self.table_env = StreamTableEnvironment.create(self.env, self.settings)
        self.logger = logging.getLogger(__name__)

    def monitor_tasks(self):
        """
        监控爬虫任务状态。
        """
        self.logger.info("Starting Flink monitoring...")
        # 模拟任务状态数据
        task_data = [("task1", "running"), ("task2", "completed")]
        data_stream = self.env.from_collection(task_data)
        data_stream.print()
        self.env.execute("Flink Monitoring Job")


def get_flink_env():
    """
    获取 Flink 执行环境。
    :return: Flink TableEnvironment 实例。
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    return StreamTableEnvironment.create(env, settings)

class AlertSystem:
    def __init__(self, smtp_server, smtp_port, sender_email, sender_password):
        """
        初始化报警系统。
        :param smtp_server: SMTP 服务器地址。
        :param smtp_port: SMTP 服务器端口。
        :param sender_email: 发件人邮箱。
        :param sender_password: 发件人邮箱密码。
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.logger = logging.getLogger(__name__)

    def send_alert(self, receiver_email, subject, message):
        """
        发送报警邮件。
        :param receiver_email: 收件人邮箱。
        :param subject: 邮件主题。
        :param message: 邮件内容。
        """
        try:
            msg = MIMEText(message)
            msg["Subject"] = subject
            msg["From"] = self.sender_email
            msg["To"] = receiver_email

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, receiver_email, msg.as_string())
            self.logger.info(f"Alert email sent to {receiver_email}.")
        except Exception as e:
            self.logger.error(f"Failed to send alert email: {e}")

def get_alert_system(smtp_server, smtp_port, sender_email, sender_password):
    """
    获取报警系统实例。
    :return: AlertSystem 实例。
    """
    return AlertSystem(smtp_server, smtp_port, sender_email, sender_password)