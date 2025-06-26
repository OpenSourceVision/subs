#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import json
import requests
import time
import logging
import yaml
import re
from typing import List, Dict, Optional, Set, Tuple
from urllib.parse import urlparse
from collections import defaultdict

# 日志配置函数
def setup_logging(log_file: str, log_level: str) -> logging.Logger:
    """配置日志系统"""
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
    """简化的节点订阅管理器"""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """初始化管理器"""
        temp_logger = logging.getLogger(__name__)
        self.config = self.load_yaml(config_path, temp_logger)
        self.logger = setup_logging(
            self.config['logging']['file'],
            self.config['logging']['level']
        )
        
        # 初始化HTTP会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['http']['user_agent']
        })
        self.timeout = self.config['http']['timeout']
        self.rate_limit_delay = self.config['http']['rate_limit_delay']
        
        # 加载映射表
        self.flags_map = self.load_flags_map('flags.yaml')
        self.region_map = self.load_region_map('region.yaml')

    def load_yaml(self, file_path: str, logger: logging.Logger) -> Dict:
        """加载YAML配置文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            logger.info(f"成功加载配置文件: {file_path}")
            return config_data
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise

    def load_flags_map(self, flags_path: str) -> dict:
        """加载国旗映射表"""
        try:
            with open(flags_path, 'r', encoding='utf-8') as f:
                flags_data = yaml.safe_load(f)
            flags_map = flags_data.get('flags', {}) if isinstance(flags_data, dict) else {}
            self.logger.info(f"成功加载国旗映射表，共有 {len(flags_map)} 个国家码")
            return flags_map
        except Exception as e:
            self.logger.warning(f"加载国旗映射表失败: {e}")
            return {}

    def load_region_map(self, region_path: str) -> dict:
        """加载region映射表"""
        try:
            with open(region_path, 'r', encoding='utf-8') as f:
                region_data = yaml.safe_load(f)
            region_map = region_data.get('region', {}) if isinstance(region_data, dict) else {}
            self.logger.info(f"成功加载region映射表，共有 {len(region_map)} 个后缀")
            return region_map
        except Exception as e:
            self.logger.warning(f"加载region映射表失败: {e}")
            return {}

    def rate_limited_request(self, url: str) -> Optional[requests.Response]:
        """发送限速的HTTP GET请求"""
        try:
            time.sleep(self.rate_limit_delay)
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            self.logger.warning(f"请求失败: {url}, 错误: {e}")
            return None

    def read_subscription_urls(self) -> List[str]:
        """读取订阅链接文件"""
        try:
            with open(self.config['files']['subscriptions'], 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 支持YAML列表或文本格式
            try:
                data = yaml.safe_load(content)
                if isinstance(data, list):
                    urls = [str(url).strip() for url in data if isinstance(url, str) and url.strip() and not url.strip().startswith('#')]
                else:
                    urls = [line.strip() for line in content.splitlines() if line.strip() and not line.strip().startswith('#')]
            except:
                urls = [line.strip() for line in content.splitlines() if line.strip() and not line.strip().startswith('#')]
            
            url_pattern = re.compile(r'^https?://[^\s]+$')
            valid_urls = [url for url in urls if url_pattern.match(url)]
            self.logger.info(f"读取到 {len(valid_urls)} 个有效订阅链接")
            return valid_urls
        except Exception as e:
            self.logger.error(f"读取订阅文件失败: {e}")
            return []

    def fetch_subscription(self, url: str) -> List[str]:
        """拉取订阅内容"""
        self.logger.info(f"正在拉取订阅 {url}...")
        response = self.rate_limited_request(url)
        if response is None:
            return []
        
        try:
            text = response.text.strip()
            if text:
                # 尝试base64解码
                padded = text + '=' * (-len(text) % 4)
                try:
                    content = base64.b64decode(padded).decode('utf-8')
                except:
                    content = text
            else:
                content = ""
        except Exception as e:
            self.logger.error(f"解码订阅内容失败: {e}")
            content = ""
        
        nodes = [line.strip() for line in content.split('\n') if line.strip() and not line.strip().startswith('#')]
        self.logger.info(f"从 {url} 获取到 {len(nodes)} 个节点")
        return nodes

    def generate_node_key(self, node_info: Dict) -> str:
        """生成节点的唯一标识符用于去重"""
        try:
            # 使用协议+主机+端口作为唯一标识
            host = node_info.get('host', '').lower()
            port = str(node_info.get('port', ''))
            protocol = node_info.get('protocol', '')
            
            # 对于vmess协议，还需要考虑更多参数
            if protocol == 'vmess' and 'config' in node_info:
                config = node_info['config']
                user_id = config.get('id', '')
                # 使用前8位UUID作为标识的一部分
                user_id_short = user_id[:8] if user_id else ''
                return f"{protocol}://{host}:{port}:{user_id_short}"
            else:
                return f"{protocol}://{host}:{port}"
        except Exception:
            return node_info.get('uri', '')

    def deduplicate_nodes(self, parsed_nodes: List[Dict]) -> List[Dict]:
        """对解析后的节点进行去重"""
        seen_keys: Set[str] = set()
        unique_nodes = []
        duplicates_count = 0
        
        for node in parsed_nodes:
            node_key = self.generate_node_key(node)
            if node_key not in seen_keys:
                seen_keys.add(node_key)
                unique_nodes.append(node)
            else:
                duplicates_count += 1
        
        self.logger.info(f"去重完成：保留 {len(unique_nodes)} 个唯一节点，移除 {duplicates_count} 个重复节点")
        return unique_nodes

    def get_unique_ips(self, nodes: List[Dict]) -> Set[str]:
        """获取所有需要查询的唯一IP地址"""
        unique_ips = set()
        for node in nodes:
            host = node.get('host', '')
            if re.match(r'^(\d{1,3}\.){3}\d{1,3}$', host):
                unique_ips.add(host)
        return unique_ips

    def batch_query_ip_locations(self, ip_addresses: Set[str]) -> Dict[str, Dict]:
        """批量查询IP地理位置"""
        ip_locations = {}
        total_ips = len(ip_addresses)
        
        self.logger.info(f"开始批量查询 {total_ips} 个唯一IP的地理位置...")
        
        for i, ip in enumerate(ip_addresses, 1):
            self.logger.info(f"查询进度: {i}/{total_ips} - {ip}")
            location_info = self.get_ip_location(ip)
            ip_locations[ip] = location_info
            
            # 每10个IP显示一次进度
            if i % 10 == 0 or i == total_ips:
                self.logger.info(f"IP查询进度: {i}/{total_ips} 已完成")
        
        self.logger.info(f"批量IP查询完成，共查询 {len(ip_locations)} 个IP")
        return ip_locations

    def get_ip_location(self, ip: str) -> Dict:
        """查询IP地理位置"""
        url = f"http://ip-api.com/json/{ip}?lang=zh-CN"
        response = self.rate_limited_request(url)
        if response is None:
            return {'country_code': 'XX', 'country': '未知', 'city': ''}
        
        try:
            data = response.json()
            country = data.get('country', '未知')
            city = data.get('city', '')
            country_code = data.get('countryCode', 'XX')
            
            # 特殊处理港澳台
            if '香港' in country or '香港' in city:
                country_code = 'HK'
                country = '香港'
            elif '澳门' in country or '澳门' in city:
                country_code = 'MO'
                country = '澳门'
            elif '台湾' in country or '台湾' in city:
                country_code = 'TW'
                country = '台湾'
            
            return {
                'country_code': country_code,
                'country': country,
                'city': city
            }
        except Exception as e:
            self.logger.warning(f"IP归属地解析失败: {ip}, 错误: {e}")
            return {'country_code': 'XX', 'country': '未知', 'city': ''}

    def get_domain_location(self, domain: str, original_name: str) -> Dict:
        """根据域名从region.yaml获取地理位置，或从原始名称中提取"""
        # 首先尝试从region.yaml匹配
        parts = domain.split('.')
        if len(parts) >= 2:
            # 尝试匹配完整后缀
            for i in range(len(parts) - 1):
                suffix = '.' + '.'.join(parts[i:])
                if suffix in self.region_map:
                    region_info = self.region_map[suffix]
                    self.logger.info(f"域名 {domain} 匹配后缀 {suffix}")
                    return {
                        'country_code': region_info.get('country_code', 'XX'),
                        'country': region_info.get('country', '未知'),
                        'city': ''
                    }
        
        # 如果region.yaml无法匹配，尝试从原始名称中提取地区信息
        if original_name:
            # 定义常见地区关键词映射
            region_keywords = {
                '香港': {'country_code': 'HK', 'country': '香港'},
                'HK': {'country_code': 'HK', 'country': '香港'},
                'Hong Kong': {'country_code': 'HK', 'country': '香港'},
                '台湾': {'country_code': 'TW', 'country': '台湾'},
                'TW': {'country_code': 'TW', 'country': '台湾'},
                'Taiwan': {'country_code': 'TW', 'country': '台湾'},
                '日本': {'country_code': 'JP', 'country': '日本'},
                'JP': {'country_code': 'JP', 'country': '日本'},
                'Japan': {'country_code': 'JP', 'country': '日本'},
                '新加坡': {'country_code': 'SG', 'country': '新加坡'},
                'SG': {'country_code': 'SG', 'country': '新加坡'},
                'Singapore': {'country_code': 'SG', 'country': '新加坡'},
                '美国': {'country_code': 'US', 'country': '美国'},
                'US': {'country_code': 'US', 'country': '美国'},
                'USA': {'country_code': 'US', 'country': '美国'},
                '韩国': {'country_code': 'KR', 'country': '韩国'},
                'KR': {'country_code': 'KR', 'country': '韩国'},
                'Korea': {'country_code': 'KR', 'country': '韩国'},
            }
            
            for keyword, info in region_keywords.items():
                if keyword in original_name:
                    self.logger.info(f"从原始名称 '{original_name}' 中提取到地区: {info['country']}")
                    return info
        
        self.logger.warning(f"域名 {domain} 无法确定地区，保持原名称")
        return None

    def get_country_flag(self, country_code: str) -> str:
        """获取国旗emoji"""
        cc = country_code.upper()
        if cc in self.flags_map:
            return self.flags_map[cc]
        if len(cc) == 2 and cc.isalpha():
            return chr(ord(cc[0]) + 127397) + chr(ord(cc[1]) + 127397)
        return ''

    def generate_ip_node_name(self, country_code: str, country: str, city: str, country_counts: Dict) -> str:
        """为IP节点生成名称：国旗+地区+城市+序号"""
        flag = self.get_country_flag(country_code)
        count_key = f"{country}{city}"
        country_counts[count_key] += 1
        sequence = f"{country_counts[count_key]:02d}"
        
        if city:
            name = f"{flag} {country} {city} {sequence}".strip()
        else:
            name = f"{flag} {country} {sequence}".strip()
        
        return name

    def generate_domain_node_name(self, country_code: str, country: str, region_counts: Dict) -> str:
        """为域名节点生成名称：国旗+地区+序号"""
        flag = self.get_country_flag(country_code)
        region_counts[country] += 1
        sequence = f"{region_counts[country]:02d}"
        name = f"{flag} {country} {sequence}".strip()
        return name

    def parse_node_info(self, node_uri: str) -> Optional[Dict]:
        """解析节点URI"""
        try:
            if not isinstance(node_uri, str) or not node_uri:
                return None
            
            # 提取原始名称
            original_name = ""
            if '#' in node_uri:
                original_name = node_uri.split('#')[-1]
            
            if node_uri.startswith('vmess://'):
                try:
                    padded = node_uri[8:] + '=' * (-len(node_uri[8:]) % 4)
                    data = base64.b64decode(padded).decode('utf-8')
                    config = json.loads(data)
                    if not config.get('add') or not config.get('port'):
                        return None
                    original_name = config.get('ps', original_name)
                    return {
                        'protocol': 'vmess',
                        'host': config.get('add', ''),
                        'port': str(config.get('port', '')),
                        'config': config,
                        'uri': node_uri,
                        'original_name': original_name
                    }
                except Exception:
                    return None
            elif node_uri.startswith(('vless://', 'trojan://', 'ss://', 'hysteria://', 'hy://', 'hysteria2://', 'hy2://')):
                parsed = urlparse(node_uri)
                if not parsed.hostname or not parsed.port:
                    return None
                protocol = node_uri.split('://')[0]
                return {
                    'protocol': protocol,
                    'host': parsed.hostname,
                    'port': str(parsed.port),
                    'uri': node_uri,
                    'original_name': original_name
                }
            return None
        except Exception:
            return None

    def update_node_name(self, node_info: Dict, new_name: str) -> str:
        """更新节点名称"""
        try:
            if node_info['protocol'] == 'vmess':
                config = node_info['config'].copy()
                config['ps'] = new_name
                new_data = base64.b64encode(json.dumps(config, ensure_ascii=False).encode('utf-8')).decode('utf-8')
                return f"vmess://{new_data}"
            else:
                uri = node_info['uri']
                if '#' in uri:
                    uri = uri.split('#')[0]
                return f"{uri}#{new_name}"
        except Exception:
            return node_info['uri']

    def process_nodes(self) -> List[str]:
        """主处理流程"""
        self.logger.info("开始处理节点...")
        
        # 拉取所有订阅
        subscription_urls = self.read_subscription_urls()
        if not subscription_urls:
            return []
        
        all_nodes = []
        for url in subscription_urls:
            nodes = self.fetch_subscription(url)
            all_nodes.extend(nodes)
        
        self.logger.info(f"共收集到 {len(all_nodes)} 个节点")
        
        # 解析节点
        parsed_nodes = []
        for node_uri in all_nodes:
            node_info = self.parse_node_info(node_uri)
            if node_info:
                parsed_nodes.append(node_info)
        
        self.logger.info(f"成功解析 {len(parsed_nodes)} 个节点")
        
        # 去重处理
        unique_nodes = self.deduplicate_nodes(parsed_nodes)
        
        # 获取所有需要查询的唯一IP
        unique_ips = self.get_unique_ips(unique_nodes)
        
        # 批量查询IP地理位置
        ip_locations = self.batch_query_ip_locations(unique_ips) if unique_ips else {}
        
        # 处理节点重命名
        country_counts = defaultdict(int)  # IP节点计数
        region_counts = defaultdict(int)   # 域名节点计数
        results = []
        
        for node in unique_nodes:
            host = node.get('host', '')
            original_name = node.get('original_name', '')
            
            # 判断是IP还是域名
            is_ip = bool(re.match(r'^(\d{1,3}\.){3}\d{1,3}$', host))
            
            if is_ip:
                # IP节点：使用预查询的位置信息
                location_info = ip_locations.get(host, {'country_code': 'XX', 'country': '未知', 'city': ''})
                new_name = self.generate_ip_node_name(
                    location_info['country_code'],
                    location_info['country'],
                    location_info['city'],
                    country_counts
                )
                new_uri = self.update_node_name(node, new_name)
                results.append({
                    'uri': new_uri,
                    'country': location_info['country_code'],
                    'name': new_name
                })
            else:
                # 域名节点：尝试匹配原名称或region.yaml
                location_info = self.get_domain_location(host, original_name)
                if location_info:
                    new_name = self.generate_domain_node_name(
                        location_info['country_code'],
                        location_info['country'],
                        region_counts
                    )
                    new_uri = self.update_node_name(node, new_name)
                    results.append({
                        'uri': new_uri,
                        'country': location_info['country_code'],
                        'name': new_name
                    })
                else:
                    # 保持原名称
                    results.append({
                        'uri': node['uri'],
                        'country': 'ZZ',
                        'name': original_name or 'Unknown'
                    })
        
        # 按国家和名称排序
        sorted_nodes = sorted(results, key=lambda x: (x['country'], x['name']))
        valid_nodes = [n['uri'] for n in sorted_nodes]
        
        self.logger.info(f"最终处理完成 {len(valid_nodes)} 个节点")
        return valid_nodes

    def write_nodes_to_file(self, nodes: List[str]) -> None:
        """将节点写入文件"""
        try:
            if not nodes:
                self.logger.warning("没有节点可写入")
                return

            # 获取北京时间
            beijing_time = time.gmtime(time.time() + 8 * 3600)
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", beijing_time)
            update_line = f"# 更新时间: {update_time} (北京时间)"

            nodes_text = '\n'.join(nodes)
            nodes_base64 = base64.b64encode(nodes_text.encode('utf-8')).decode('utf-8')

            with open('Node.txt', 'w', encoding='utf-8') as f:
                f.write(f"{update_line}\n{nodes_base64}")

            self.logger.info(f"成功将 {len(nodes)} 个节点以base64格式写入 Node.txt")

            # --- 推送结果到README.md最前面 ---
            self.push_result_to_readme(update_time, len(nodes))
        except Exception as e:
            self.logger.error(f"写入节点文件失败: {e}")

    def push_result_to_readme(self, update_time: str, node_count: int) -> None:
        """将结果推送到README.md文件最前面"""
        try:
            readme_path = 'README.md'
            with open(readme_path, 'r', encoding='utf-8') as f:
                content = f.read()
            # 移除已有的推送信息（如果存在）
            content = re.sub(
                r"(^\*\*最近更新时间（北京时间）\*\*: .+?\n\n\*\*已处理节点数\*\*: .+\n*)",
                "",
                content,
                flags=re.MULTILINE
            )
            push_text = f"**最近更新时间（北京时间）**: {update_time}\n\n**已处理节点数**: {node_count}\n\n"
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write(push_text + content.lstrip())
            self.logger.info("已将结果推送到README.md最前面")
        except Exception as e:
            self.logger.error(f"推送结果到README.md失败: {e}")

    def run(self):
        """主程序入口"""
        self.logger.info("开始节点订阅管理任务...")
        try:
            processed_nodes = self.process_nodes()
            if processed_nodes:
                self.write_nodes_to_file(processed_nodes)
                self.logger.info("任务成功完成！")
            else:
                self.logger.warning("未处理任何节点")
        except Exception as e:
            self.logger.error(f"执行任务失败: {e}")
            raise

if __name__ == "__main__":
    try:
        manager = NodeSubscriptionManager()
        manager.run()
    except Exception as e:
        logging.getLogger(__name__).error(f"程序启动失败: {e}")
