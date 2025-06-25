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
from typing import List, Dict, Optional
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import logging
from requests.exceptions import RequestException, Timeout, HTTPError

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
    节点订阅管理器：负责拉取、解析、去重、命名和输出节点。
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
            self.config = self.load_config(config_path, temp_logger)
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
            self.logger.info(f"成功加载国旗映射表，共有{len(flags_map)}个国家码")
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

    def load_config(self, config_path: str, logger: logging.Logger) -> Dict:
        """
        Load YAML configuration file.
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"Successfully loaded configuration from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file {config_path} not found")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse configuration file {config_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")
            raise

    def rate_limited_request(self, url: str, headers: Optional[Dict] = None) -> Optional[requests.Response]:
        """
        HTTP GET request with rate limiting and retries.
        """
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.rate_limit_delay)
                response = self.session.get(url, headers=headers, timeout=self.timeout)
                response.raise_for_status()
                return response
            except Timeout:
                self.logger.warning(f"Request timed out {url} (attempt {attempt + 1}/{self.max_retries})")
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
        仅用Google DNS解析域名，返回IP。
        日志输出解析结果。
        """
        headers = {'Accept': 'application/dns-json'}
        url = f"https://dns.google/resolve?name={domain}&type=A"
        response = self.rate_limited_request(url, headers)
        if response is None:
            self.logger.warning(f"DNS解析失败: {domain}")
            return None
        try:
            data = response.json()
            if data.get('Answer'):
                for answer in data['Answer']:
                    if answer.get('type') == 1:
                        self.logger.debug(f"Google DNS解析成功: {domain} -> {answer.get('data')}")
                        return answer.get('data')
        except Exception:
            self.logger.warning(f"Google DNS解析异常: {domain}")
            return None
        self.logger.warning(f"Google DNS解析失败: {domain}")
        return None

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
                self.logger.warning(f"IP归属地API返回IP不符: 期望{ip}, 实际{data.get('query')}")
                return {'country_code': 'XX', 'country': '未知', 'city': '', 'ip': ip}
            country = data.get('country', '未知')
            city = data.get('city', '')
            country_code = data.get('countryCode', 'XX')
            # 特殊地区处理
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
            # 优先用flags.yaml
            if cc in self.flags_map:
                return self.flags_map[cc]
            # 兜底用unicode算法
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
        Update node name and return new URI.
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
            self.logger.warning(f"Cannot update node name, unknown protocol: {node_info.get('protocol')}")
            return node_info['uri']
        except (KeyError, json.JSONDecodeError, base64.binascii.Error) as e:
            self.logger.warning(f"Failed to update node name: {e}")
            return node_info['uri']

    def process_nodes(self) -> List[str]:
        """
        Main processing pipeline: fetch, parse, deduplicate, name, and sort nodes, return node URI list.
        """
        self.logger.info("Starting node processing...")
        subscription_urls = self.read_subscription_urls()
        if not subscription_urls:
            self.logger.error("No subscription URLs found")
            return []
        all_nodes = []
        for url in subscription_urls:
            nodes = self.fetch_subscription(url)
            all_nodes.extend(nodes)
        self.logger.info(f"Collected {len(all_nodes)} nodes in total")
        parsed_nodes = []
        for node_uri in all_nodes:
            node_info = self.parse_node_info(node_uri)
            if node_info:
                parsed_nodes.append(node_info)
        self.logger.info(f"Successfully parsed {len(parsed_nodes)} nodes")
        unique_nodes = {}
        country_counts = defaultdict(int)
        def process_node(node_info: Dict) -> Optional[Dict]:
            host = node_info.get('host', '')
            port = node_info.get('port', '')
            protocol = node_info.get('protocol', '')
            if not host or not port or not protocol:
                self.logger.warning(f"Skipping invalid node: {node_info.get('uri', '')[:50]}...")
                return None
            if re.match(r'^(\d{1,3}\.){3}\d{1,3}$', host):
                ip = host
            else:
                ip = self.resolve_domain_with_dns(host)
                if not ip:
                    self.logger.warning(f"Failed to resolve domain {host}, skipping node")
                    return None
            if protocol == "vless":
                parsed = urlparse(node_info['uri'])
                uuid = parsed.username or ''
                if not uuid:
                    self.logger.warning(f"VLESS node missing UUID: {node_info['uri'][:50]}...")
                    return None
                unique_key = f"{protocol}:{ip}:{port}:{uuid}"
            else:
                unique_key = f"{protocol}:{ip}:{port}"
            if unique_key in unique_nodes and not self.force_update:
                self.logger.debug(f"Skipping duplicate node: {ip}:{port} ({protocol}), URI: {node_info['uri'][:50]}...")
                return None
            location_info = self.get_ip_location(ip)
            country_code = location_info.get('country_code', 'XX')
            country = location_info.get('country', '未知')
            city = location_info.get('city', '')
            new_name = self.generate_unique_name(country_code, country, city, country_counts)
            new_uri = self.update_node_name(node_info, new_name)
            unique_nodes[unique_key] = True
            return {
                'uri': new_uri,
                'country': country_code,
                'name': new_name,
                'ip': ip,
                'port': port
            }
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(executor.map(process_node, parsed_nodes))
        unique_nodes_list = [result for result in results if result is not None]
        self.logger.info(f"After deduplication, {len(unique_nodes_list)} nodes remain")
        sorted_nodes = sorted(unique_nodes_list, key=lambda x: (x['country'], x['name']))
        return [node['uri'] for node in sorted_nodes]

    def write_nodes_to_file(self, nodes: List[str]) -> None:
        """
        Encode node URI list as base64 and write to Node.txt file.
        """
        try:
            if not nodes:
                self.logger.warning("No nodes to write, skipping file creation")
                return
            nodes_text = '\n'.join(nodes)
            nodes_base64 = base64.b64encode(nodes_text.encode('utf-8')).decode('utf-8')
            with open('Node.txt', 'w', encoding='utf-8') as f:
                f.write(nodes_base64)
            self.logger.info(f"Successfully wrote {len(nodes)} nodes as base64 to Node.txt")
        except (PermissionError, OSError) as e:
            self.logger.error(f"Error writing node file: {e}")

    def check_api_availability(self) -> bool:
        """
        Check if IP geolocation API is available.
        """
        try:
            response = self.rate_limited_request("http://ip-api.com/json/8.8.8.8?lang=zh-CN")
            if response and response.status_code == 200:
                self.logger.info("IP geolocation API is available")
                return True
            self.logger.warning("IP geolocation API is unavailable")
            return False
        except RequestException as e:
            self.logger.warning(f"Failed to check API availability: {e}")
            return False

    def run(self):
        """
        Main program entry point, execute full pipeline.
        """
        self.logger.info("Starting node subscription management task...")
        try:
            if not self.check_api_availability():
                self.logger.warning("IP geolocation API is unavailable, program may not function normally")
            processed_nodes = self.process_nodes()
            if processed_nodes:
                self.write_nodes_to_file(processed_nodes)
                self.logger.info("Task completed successfully!")
            else:
                self.logger.warning("No nodes were processed")
        except Exception as e:
            self.logger.error(f"Error executing task: {e}")
            raise

    def parse_node_info(self, node_uri: str) -> Optional[Dict]:
        """
        Parse a single node URI, return standardized dictionary.
        """
        try:
            if not isinstance(node_uri, str) or not node_uri:
                self.logger.warning(f"Invalid node URI: {str(node_uri)[:50]}...")
                return None
            if node_uri.startswith('vmess://'):
                try:
                    padded = node_uri[8:] + '=' * (-len(node_uri[8:]) % 4)
                    data = base64.b64decode(padded).decode('utf-8')
                    config = json.loads(data)
                    if not config.get('add') or not config.get('port'):
                        self.logger.warning(f"VMess node missing required fields: {node_uri[:50]}...")
                        return None
                    return {
                        'protocol': 'vmess',
                        'host': config.get('add', ''),
                        'port': str(config.get('port', '')),
                        'config': config,
                        'uri': node_uri
                    }
                except (base64.binascii.Error, json.JSONDecodeError, UnicodeDecodeError) as e:
                    self.logger.warning(f"Failed to parse VMess node: {node_uri[:50]}..., error: {e}")
                    return None
            elif node_uri.startswith('vless://'):
                parsed = urlparse(node_uri)
                if not parsed.hostname or not parsed.port:
                    self.logger.warning(f"VLESS node format error: {node_uri[:50]}...")
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
                    self.logger.warning(f"Trojan node format error: {node_uri[:50]}...")
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
                    self.logger.warning(f"Shadowsocks node format error: {node_uri[:50]}...")
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
                    self.logger.warning(f"Failed to parse Shadowsocks node: {node_uri[:50]}..., error: {e}")
                    return None
            elif node_uri.startswith(('hysteria2://', 'hy2://')):
                parsed = urlparse(node_uri)
                if not parsed.hostname or not parsed.port:
                    self.logger.warning(f"Hysteria2 node format error: {node_uri[:50]}...")
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
                    self.logger.warning(f"Hysteria node format error: {node_uri[:50]}...")
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