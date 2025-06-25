#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os
import base64
import json
import requests
import time
import logging
import yaml
import socket
import ssl
from typing import List, Dict, Optional
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import RequestException, Timeout, HTTPError
import dns.resolver

# 日志配置函数：初始化日志到文件和控制台
def setup_logging(log_file: str, log_level: str) -> logging.Logger:
    """
    配置日志系统，支持文件和控制台输出，覆盖旧日志内容。
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    if logger.hasHandlers():
        logger.handlers.clear()
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

class NodeSubscriptionManager:
    """
    节点订阅管理器：负责拉取、解析、去重、命名、验证和输出节点。
    """
    def __init__(self, config_path: str = 'config.yaml'):
        """
        初始化管理器，加载配置、日志和HTTP会话，并加载国旗映射。
        """
        self.logger = None
        self.session = None
        try:
            # 先初始化临时日志记录器
            temp_logger = logging.getLogger(__name__)
            self.config = self.load_yaml(config_path, temp_logger)
            self.logger = setup_logging(
                self.config['logging']['file'],
                self.config['logging']['level']
            )
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': self.config['http']['user_agent']
            })
            self.max_retries = self.config['http']['max_retries']
            self.timeout = self.config['http']['timeout']
            self.rate_limit_delay = self.config['http']['rate_limit_delay']
            self.max_delay = self.config['http']['max_delay']
            self.max_workers = self.config['http']['max_workers']
            self.force_update = os.getenv('FORCE_UPDATE', 'false').lower() == 'true'
            # 加载国旗映射
            self.flags_map = self.load_flags_map('flags.yaml')
            self.logger.info(f"Force update mode: {self.force_update}")
        except Exception as e:
            logging.getLogger(__name__).error(f"初始化失败: {e}")
            raise

    def load_flags_map(self, flags_path: str) -> dict:
        """
        加载国旗映射表（flags.yaml），返回国家码到国旗emoji的映射字典。
        """
        try:
            with open(flags_path, 'r', encoding='utf-8') as f:
                flags_data = yaml.safe_load(f)
            flags_map = flags_data.get('flags', {}) if isinstance(flags_data, dict) else {}
            self.logger.info(f"成功加载国旗映射表，共有 {len(flags_map)} 个国家码")
            return flags_map
        except Exception as e:
            self.logger.warning(f"加载国旗映射表失败: {e}")
            return {}

    def __del__(self):
        """
        析构器，关闭HTTP会话。
        """
        if hasattr(self, 'session') and self.session is not None:
            self.session.close()
            self.logger.debug("HTTP session closed successfully")
            self.logger.debug("HTTP session closed successfully.")

    def load_yaml(self, file_path: str, logger: logging.Logger) -> Dict:
        """
        Load YAML configuration file.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            logger.info(f"Successfully loaded configuration from {file_path}")
            return config_data
        except FileNotFoundError:
            logger.error(f"Configuration file not {file_path} not found")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse configuration file {file_path}: {e}")
            raise
        except Exception as e:
            logger.error("Error loading configuration file: {e}")
            raise

    def rate_limited_request(self, url: str, headers: Optional[Dict] = None) -> Optional[requests.Response]:
        """
        HTTP GET request with rate limiting and retries limit.
        """
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.rate_limit_delay)
                response = self.session.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                return response
            except TimeoutError:
                self.logger.warning(f"Request timed out for{url} on (attempt {attempt + 1}/{self.max_retries})")
            except HTTPError as e:
                self.logger.warning(f"HTTP error on {url}: {e} (attempt {attempt + 1}/{self.max_retries})")
            except RequestException as e:
                self.logger.warning(f"Request failed {url}: {e} (attempt {attempt + 1}/{self.max_retries})")
            if attempt < self.max_retries - 1:
                time.sleep(min(2 ** attempt, self.max_delay))
        self.logger.error(f"Failed to retrieve {url} after {self.max_retries} attempts")
        return None

    def read_subscription_urls(self) -> List[str]:
        """
        读取订阅链接文件，支持YAML列表或每行一个URL的文本。
        日志输出读取到的有效订阅数量。
        """
        try:
            with open(self.config['files']['subscriptions'], 'r', encoding='utf-8') as f:
                content = f.read()
            try:
                data = yaml.safe_load(content)
                if isinstance(data, list):
                    urls = [str(url).strip() for url in data if isinstance(url, str) and url.strip() and not url.strip().startswith('#')]
                else:
                    urls = [line.strip() for line in content.splitlines() if line.strip() and not line.strip().startswith('#')]
            except Exception:
                urls = [line.strip() for line in content.splitlines() if line.strip() and not line.strip().startswith('#')]
            url_pattern = re.compile(r'^https?://[^\s]+$')
            valid_urls = [url for url in urls if url_pattern.match(url)]
            if not valid_urls:
                self.logger.error(f"{self.config['files']['subscriptions']} 文件未找到有效订阅URL")
            else:
                self.logger.info(f"读取到 {len(valid_urls)} 个有效订阅链接")
            return valid_urls
        except FileNotFoundError:
            self.logger.error(f"文件 {self.config['files']['subscriptions']} 不存在")
            return []
        except PermissionError:
            self.logger.error(f"无权限访问文件 {self.config['files']['subscriptions']}")
            return []
        except Exception as e:
            self.logger.error(f"读取订阅文件时出错: {e}")
            return []

    def fetch_subscription(self, url: str) -> List[str]:
        """
        拉取订阅内容，自动解码base64或直接读取文本。
        日志输出拉取和解码情况。
        """
        self.logger.info(f"正在拉取订阅 {url} ...")
        response = self.rate_limited_request(url)
        if response is None:
            self.logger.warning(f"拉取订阅失败: {url}")
            return []
        try:
            text = response.text.strip()
            if text:
                padded = text + '=' * (-len(text) % 4)
                try:
                    content = base64.b64decode(padded).decode('utf-8')
                    self.logger.debug("成功解码base64内容")
                except (base64.binascii.Error, UnicodeDecodeError):
                    content = text
                    self.logger.debug("使用原始文本内容")
            else:
                content = ""
        except Exception as e:
            self.logger.error(f"解码订阅内容失败: {e}")
            content = ""
        nodes = [line.strip() for line in content.split('\n') if line.strip() and not line.strip().startswith('#')]
        self.logger.info(f"从 {url} 获取到 {len(nodes)} 个节点")
        return nodes

    def resolve_domain_with_dns(self, domain: str) -> Optional[str]:
        """
        使用多个DNS服务解析域名，返回IP，优先尝试非CDN IP。
        日志输出解析结果。
        """
        self.logger.debug(f"开始解析域名: {domain}")
        dns_servers = [
            ('Google DNS', '8.8.8.8'),
            ('Cloudflare DNS', '1.1.1.1'),
            ('Quad9 DNS', '9.9.9.9')
        ]
        resolver = dns.resolver.Resolver()
        ip = None
        ttl = None
        for dns_name, dns_ip in dns_servers:
            try:
                resolver.nameservers = [dns_ip]
                answers = resolver.resolve(domain, 'A')
                for rdata in answers:
                    ip = rdata.address
                    ttl = rdata.ttl
                    self.logger.debug(f"{dns_name} 解析: {domain} -> {ip} (TTL: {ttl})")
                    if ttl < 300:
                        self.logger.warning(f"{domain} 可能为CDN节点，IP: {ip}, TTL: {ttl}")
                    else:
                        return ip
            except Exception as e:
                self.logger.warning(f"{dns_name} 解析失败: {domain}, 错误: {e}")
        
        # 尝试MX记录
        mx_ip = self.resolve_mx_record(domain)
        if mx_ip:
            self.logger.info(f"通过MX记录获取IP: {domain} -> {mx_ip}")
            return mx_ip
        
        # 尝试子域名
        subdomains = [f'direct.{domain}', f'www.{domain}', f'mail.{domain}']
        for subdomain in subdomains:
            try:
                answers = resolver.resolve(subdomain, 'A')
                for rdata in answers:
                    ip = rdata.address
                    self.logger.debug(f"子域名解析: {subdomain} -> {ip}")
                    return ip
            except Exception:
                self.logger.debug(f"子域名解析失败: {subdomain}")
        
        if ip:
            self.logger.warning(f"未找到非CDN IP，使用最后解析结果: {domain} -> {ip}")
            return ip
        self.logger.warning(f"所有DNS解析失败: {domain}")
        return None

    def resolve_mx_record(self, domain: str) -> Optional[str]:
        """
        查询MX记录，尝试获取非CDN IP。
        """
        try:
            resolver = dns.resolver.Resolver()
            answers = resolver.resolve(domain, 'MX')
            for rdata in answers:
                mx_host = str(rdata.exchange).rstrip('.')
                self.logger.debug(f"找到MX记录: {domain} -> {mx_host}")
                ip = self.resolve_domain_with_dns(mx_host)
                if ip:
                    self.logger.info(f"MX记录解析成功: {mx_host} -> {ip}")
                    return ip
            return None
        except Exception as e:
            self.logger.warning(f"MX记录解析失败: {domain}, 错误: {e}")
            return None

    def verify_landing_ip(self, ip: str, domain: str, port: str) -> bool:
        """
        验证IP是否为实际落地IP，通过HTTP/HTTPS请求或SSL证书检查。
        """
        self.logger.debug(f"验证落地IP: {ip}:{port} (域名: {domain})")
        try:
            for scheme in ['https', 'http']:
                url = f"{scheme}://{ip}"
                headers = {'Host': domain}
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=False
                )
                if response.status_code in (200, 301, 302):
                    self.logger.info(f"落地IP验证成功: {ip} 返回状态码 {response.status_code}")
                    return True
                self.logger.debug(f"落地IP验证失败: {ip} 返回状态码 {response.status_code}")
            
            context = ssl.create_default_context()
            with socket.create_connection((ip, port)) as sock:
                with context.wrap_socket(sock, server_hostname=domain) as ssock:
                    cert = ssock.getpeercert()
                    self.logger.debug(f"SSL证书验证成功: {ip} (域名: {domain})")
                    return True
        except (RequestException, ssl.SSLError, socket.gaierror) as e:
            self.logger.warning(f"落地IP验证失败: {ip} (域名: {domain}), 错误: {e}")
            return False

    def test_landing_ip(self, node: Dict) -> bool:
        """
        测试节点IP是否可达并返回正确响应。
        """
        ip = node['ip']
        port = node['port']
        domain = node.get('host', '')
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((ip, int(port)))
            sock.close()
            if result == 0:
                self.logger.info(f"节点 {node['name']} IP {ip}:{port} TCP连接成功")
                return self.verify_landing_ip(ip, domain, port)
            else:
                self.logger.warning(f"节点 {node['name']} IP {ip}:{port} TCP连接失败")
                return False
        except Exception as e:
            self.logger.warning(f"测试节点 {node['name']} IP {ip}:{port} 失败: {e}")
            return False

    def get_ip_location(self, ip: str) -> Dict:
        """
        查询IP地理位置，返回国家码、国家名、城市。
        日志输出查询和解析情况。
        """
        url = f"http://ip-api.com/json/{ip}?lang=zh-CN"
        response = self.rate_limited_request(url)
        if response is None:
            self.logger.warning(f"IP归属地查询失败: {ip}")
            return {'country_code': 'XX', 'country': '未知', 'city': '', 'ip': ip}
        try:
            data = response.json()
            if data.get('query') != ip:
                self.logger.warning(f"IP归属地API返回IP不符: 期望 {ip}, 实际 {data.get('query')}")
                return {'country_code': 'XX', 'country': '未知', 'city': '', 'ip': ip}
            country = data.get('country', '未知')
            city = data.get('city', '')
            country_code = data.get('countryCode', 'XX')
            if '香港' in country or '香港' in city:
                country_code = 'HK'
                country = '香港'
            elif '澳门' in country or '澳门' in city:
                country_code = 'MO'
                country = '澳门'
            elif '台湾' in country or '台湾' in city:
                country_code = 'TW'
                country = '台湾'
            if not (len(country_code) == 2 and country_code.isalpha()):
                self.logger.warning(f"无效国家码: {country_code}, IP: {ip}")
                country_code = 'XX'
            self.logger.info(f"IP {ip} 归属地: {country}({country_code}) {city}")
            return {
                'country_code': country_code,
                'country': country,
                'city': city,
                'ip': ip
            }
        except Exception as e:
            self.logger.warning(f"IP归属地API解析失败: {ip}, 错误: {e}")
            return {'country_code': 'XX', 'country': '未知', 'city': '', 'ip': ip}

    def generate_unique_name(self, country_code: str, country: str, city: str, country_counts: Dict[str, int]) -> str:
        """
        生成唯一节点名，格式为"国旗 国家 城市 序号"。
        优先从flags.yaml获取国旗emoji，获取不到则用unicode算法。
        """
        def country_flag(cc):
            cc = cc.upper()
            if cc in self.flags_map:
                return self.flags_map[cc]
            if len(cc) == 2 and cc.isalpha():
                return chr(ord(cc[0]) + 127397) + chr(ord(cc[1]) + 127397)
            return ''
        flag = country_flag(country_code)
        count_key = f"{country}{city}"
        country_counts[count_key] += 1
        sequence = f"{country_counts[count_key]:02d}"
        name = f"{flag} {country} {city} {sequence}".strip()
        self.logger.debug(f"生成节点名: {name}")
        return name

    def update_node_name(self, node_info: Dict, new_name: str) -> str:
        """
        更新节点名称并返回新的URI。
        """
        try:
            if node_info['protocol'] == 'vmess':
                config = node_info['config'].copy()
                config['ps'] = new_name
                new_data = base64.b64encode(json.dumps(config, ensure_ascii=False).encode('utf-8')).decode('utf-8')
                return f"vmess://{new_data}"
            elif node_info['protocol'] in ['vless', 'trojan', 'ss', 'hysteria', 'hysteria2']:
                uri = node_info['uri']
                if '#' in uri:
                    uri = uri.split('#')[0]
                return f"{uri}#{new_name}"
            self.logger.warning(f"无法更新节点名称，未知协议: {node_info.get('protocol')}")
            return node_info['uri']
        except (KeyError, json.JSONDecodeError, base64.binascii.Error) as e:
            self.logger.warning(f"更新节点名称失败: {e}")
            return node_info['uri']

    def process_node(self, node_info: Dict) -> Optional[Dict]:
        """
        处理单个节点：解析域名、验证IP、查询地理位置、生成名称。
        """
        host = node_info.get('host', '')
        port = node_info.get('port', '')
        protocol = node_info.get('protocol', '')
        if not host or not port or not protocol:
            self.logger.warning(f"跳过无效节点: {node_info.get('uri', '')[:50]}...")
            return None
        
        if re.match(r'^(\d{1,3}\.){3}\d{1,3}$', host):
            ip = host
        else:
            ip = self.resolve_domain_with_dns(host)
            if not ip:
                self.logger.warning(f"无法解析域名 {host}，跳过节点")
                return None
        
        if not self.verify_landing_ip(ip, host, port):
            self.logger.warning(f"IP {ip} 不是落地IP，跳过节点")
            return None
        
        if protocol == "vless":
            parsed = urlparse(node_info['uri'])
            uuid = parsed.username or ''
            if not uuid:
                self.logger.warning(f"VLESS 节点缺少 UUID: {node_info['uri'][:50]}...")
                return None
            unique_key = f"{protocol}:{ip}:{port}:{uuid}"
        else:
            unique_key = f"{protocol}:{ip}:{port}"
        
        unique_nodes = {}
        if unique_key in unique_nodes and not self.force_update:
            self.logger.debug(f"跳过重复节点: {ip}:{port} ({protocol}), URI: {node_info['uri'][:50]}...")
            return None
        
        location_info = self.get_ip_location(ip)
        country_code = location_info.get('country_code', 'XX')
        country = location_info.get('country', '未知')
        city = location_info.get('city', '')
        country_counts = defaultdict(int)
        new_name = self.generate_unique_name(country_code, country, city, country_counts)
        new_uri = self.update_node_name(node_info, new_name)
        unique_nodes[unique_key] = True
        
        return {
            'uri': new_uri,
            'country': country_code,
            'name': new_name,
            'ip': ip,
            'port': port,
            'host': host
        }

    def process_nodes(self) -> List[str]:
        """
        主处理流程：拉取、解析、去重、命名、排序、测试节点。
        """
        self.logger.info("开始处理节点...")
        subscription_urls = self.read_subscription_urls()
        if not subscription_urls:
            self.logger.error("未找到订阅 URL")
            return []
        all_nodes = []
        for url in subscription_urls:
            nodes = self.fetch_subscription(url)
            all_nodes.extend(nodes)
        self.logger.info(f"共收集到 {len(all_nodes)} 个节点")
        parsed_nodes = []
        for node_uri in all_nodes:
            node_info = self.parse_node_info(node_uri)
            if node_info:
                parsed_nodes.append(node_info)
        self.logger.info(f"成功解析 {len(parsed_nodes)} 个节点")
        unique_nodes = {}
        country_counts = defaultdict(int)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(executor.map(lambda x: self.process_node(x), parsed_nodes))
        sorted_nodes = sorted([r for r in results if r], key=lambda x: (x['country'], x['name']))
        
        valid_nodes = []
        for node in sorted_nodes:
            if self.test_landing_ip(node):
                valid_nodes.append(node['uri'])
                self.logger.info(f"节点 {node['name']} 落地IP测试通过")
            else:
                self.logger.warning(f"节点 {node['name']} 落地IP测试失败，跳过")
        
        self.logger.info(f"最终保留 {len(valid_nodes)} 个通过测试的节点")
        return valid_nodes

    def write_nodes_to_file(self, nodes: List[str]) -> None:
        """
        将节点URI列表编码为base64并写入Node.txt文件。
        """
        try:
            if not nodes:
                self.logger.warning("没有节点可写入，跳过文件创建")
                return
            nodes_text = '\n'.join(nodes)
            nodes_base64 = base64.b64encode(nodes_text.encode('utf-8')).decode('utf-8')
            with open('Node.txt', 'w', encoding='utf-8') as f:
                f.write(nodes_base64)
            self.logger.info(f"成功将 {len(nodes)} 个节点以base64格式写入 Node.txt")
        except (PermissionError, OSError) as e:
            self.logger.error(f"写入节点文件出错: {e}")

    def check_api_availability(self) -> bool:
        """
        检查IP地理位置API是否可用。
        """
        try:
            response = self.rate_limited_request("http://ip-api.com/json/8.8.8.8?lang=zh-CN")
            if response and response.status_code == 200:
                self.logger.info("IP地理位置API可用")
                return True
            self.logger.warning("IP地理位置API不可用")
            return False
        except RequestException as e:
            self.logger.warning(f"检查API可用性失败: {e}")
            return False

    def run(self):
        """
        主程序入口，执行完整流程。
        """
        self.logger.info("开始节点订阅管理任务...")
        try:
            if not self.check_api_availability():
                self.logger.warning("IP地理位置API不可用，程序可能无法正常运行")
            processed_nodes = self.process_nodes()
            if processed_nodes:
                self.write_nodes_to_file(processed_nodes)
                self.logger.info("任务成功完成！")
            else:
                self.logger.warning("未处理任何节点")
        except Exception as e:
            self.logger.error(f"执行任务出错: {e}")
            raise

    def parse_node_info(self, node_uri: str) -> Optional[Dict]:
        """
        解析单个节点URI，返回标准化的字典。
        """
        try:
            if not isinstance(node_uri, str) or not node_uri:
                self.logger.warning(f"无效节点 URI: {str(node_uri)[:50]}...")
                return None
            if node_uri.startswith('vmess://'):
                try:
                    padded = node_uri[8:] + '=' * (-len(node_uri[8:]) % 4)
                    data = base64.b64decode(padded).decode('utf-8')
                    config = json.loads(data)
                    if not config.get('add') or not config.get('port'):
                        self.logger.warning(f"VMess 节点缺少必要字段: {node_uri[:50]}...")
                        return None
                    return {
                        'protocol': 'vmess',
                        'host': config.get('add', ''),
                        'port': str(config.get('port', '')),
                        'config': config,
                        'uri': node_uri
                    }
                except (base64.binascii.Error, json.JSONDecodeError, UnicodeDecodeError) as e:
                    self.logger.warning(f"解析 VMess 节点失败: {node_uri[:50]}..., 错误: {e}")
                    return None
            elif node_uri.startswith('vless://'):
                parsed = urlparse(node_uri)
                if not parsed.hostname or not parsed.port:
                    self.logger.warning(f"VLESS 节点格式错误: {node_uri[:50]}...")
                    return None
                return {
                    'protocol': 'vless',
                    'host': parsed.hostname,
                    'port': str(parsed.port),
                    'uri': node_uri
                }
            elif node_uri.startswith('trojan://'):
                parsed = urlparse(node_uri)
                if not parsed.hostname or not parsed.port:
                    self.logger.warning(f"Trojan 节点格式错误: {node_uri[:50]}...")
                    return None
                return {
                    'protocol': 'trojan',
                    'host': parsed.hostname,
                    'port': str(parsed.port),
                    'uri': node_uri
                }
            elif node_uri.startswith('ss://'):
                if '#' in node_uri:
                    ss_part, _ = node_uri.split('#', 1)
                else:
                    ss_part = node_uri
                if '@' not in ss_part or ':' not in ss_part:
                    self.logger.warning(f"Shadowsocks 节点格式错误: {node_uri[:50]}...")
                    return None
                try:
                    method_pass, server_port = ss_part[5:].split('@', 1)
                    server, port = server_port.rsplit(':', 1)
                    return {
                        'protocol': 'ss',
                        'host': server,
                        'port': port,
                        'uri': node_uri
                    }
                except ValueError as e:
                    self.logger.warning(f"解析 Shadowsocks 节点失败: {node_uri[:50]}..., 错误: {e}")
                    return None
            elif node_uri.startswith(('hysteria2://', 'hy2://')):
                parsed = urlparse(node_uri)
                if not parsed.hostname or not parsed.port:
                    self.logger.warning(f"Hysteria2 节点格式错误: {node_uri[:50]}...")
                    return None
                return {
                    'protocol': 'hysteria2',
                    'host': parsed.hostname,
                    'port': str(parsed.port),
                    'uri': node_uri
                }
            elif node_uri.startswith(('hysteria://', 'hy://')):
                parsed = urlparse(node_uri)
                if not parsed.hostname or not parsed.port:
                    self.logger.warning(f"Hysteria {node_uri} 节点格式错误: {node_uri[:50]}...")
                    return None
                return {
                    'protocol': 'hysteria',
                    'host': parsed.hostname,
                    'port': str(parsed.port),
                    'uri': node_uri
                }
            self.logger.warning(f"Unknown protocol: {node_uri[:50]}...")
            return None
        except Exception as e:
            self.logger.warning(f"Failed to parse node: {node_uri[:50]}..., error: {e}")
            return None

if __name__ == "__main__":
    try:
        manager = NodeSubscriptionManager()
        manager.run()
    except Exception as e:
        logging.getLogger(__name__).error(f"Program startup failed: {e}")
