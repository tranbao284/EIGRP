import socket
import threading
import json
import time
import sys

PORT = 50000
HELLO_INTERVAL = 3
HOLD_TIME = 10

class InteractiveEIGRP:
    def __init__(self, router_name, local_net):
        self.router_name = router_name
        self.local_net = local_net
        
        self.bandwidth = 100000  
        self.delay = 100         
        self.load = 1            
        self.reliability = 255   
        self.link_cost = self.calculate_metric()
        
        self.neighbors = {}
        self.topology_table = {self.local_net: {self.router_name: {"RD": 0, "FD": 0}}}
        self.routing_table = {self.local_net: {"metric": 0, "next_hop": "Direct"}}
        
        self.blocked_neighbors = set()
        
        self.lock = threading.Lock()
        self.setup_sockets()

    def setup_sockets(self):
        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        
        self.recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass 
        self.recv_sock.bind(('', PORT))

    def calculate_metric(self):
        bw_metric = 10000000 / self.bandwidth
        delay_metric = self.delay
        penalty = (self.load * 10) + (255 - self.reliability) * 10
        return int(256 * (bw_metric + delay_metric)) + penalty

    def change_parameters(self):
        print("\n--- NHAP THONG SO INTERFACE ---")
        try:
            self.bandwidth = int(input("Nhap Bang thong (Kbps) [vd 100000]: "))
            self.delay = int(input("Nhap Do tre (tens of ms) [vd 100]: "))
            self.load = int(input("Nhap Tai (1-255) [vd 1]: "))
            self.reliability = int(input("Nhap Do tin cay (1-255) [vd 255]: "))
            
            old_cost = self.link_cost
            self.link_cost = self.calculate_metric()
            print(f"[!] Da cap nhat! Link Cost thay doi tu {old_cost} -> {self.link_cost}")
            
            with self.lock:
                changed = False
                for net, paths in self.topology_table.items():
                    if net == self.local_net: continue
                    for neighbor, metrics in paths.items():
                        new_fd = metrics["RD"] + self.link_cost
                        
                        if metrics["FD"] != new_fd:
                            self.topology_table[net][neighbor]["FD"] = new_fd
                            changed = True
                
                if changed:
                    self.run_dual()
                else:
                    threading.Thread(target=self.trigger_update).start()
                    
        except ValueError:
            print("[LOI] Vui long nhap so nguyen hop le!")

    def toggle_link(self):
        print("\n--- QUAN LY LINK (CAP) ---")
        print(f" Lang gieng hien tai: {list(self.neighbors.keys())}")
        print(f" Cap dang bi rut: {list(self.blocked_neighbors)}")
        
        target_input = input("Nhap ten Router muon ngat/ket noi lai (VD: R1 R2): ").strip()
        if not target_input: return
        
        targets = target_input.replace(',', ' ').split()
        
        with self.lock:
            for target in targets:
                if target in self.blocked_neighbors:
                    self.blocked_neighbors.remove(target)
                    print(f"[!] Da CAM LAI cap voi {target}.")
                else:
                    self.blocked_neighbors.add(target)
                    print(f"[!] Da RUT CAP noi voi {target}.")
        print(f"[i] Vui long cho {HOLD_TIME}s de mang tu dong hoi tu...")

    def send_hello(self):
        while True:
            # [FIX 2 CHIỀU] Gửi kèm danh sách đen để báo cho láng giềng biết mình đã cự tuyệt họ
            payload = {
                "type": "HELLO", 
                "router_name": self.router_name,
                "blocked": list(self.blocked_neighbors)
            }
            self.send_sock.sendto(json.dumps(payload).encode(), ('<broadcast>', PORT))
            time.sleep(HELLO_INTERVAL)

    def trigger_update(self):
        with self.lock:
            # [FIX 2 CHIỀU] Update cũng phải kèm danh sách đen
            payload = {
                "type": "UPDATE", 
                "router_name": self.router_name, 
                "routes": self.routing_table,
                "blocked": list(self.blocked_neighbors)
            }
        self.send_sock.sendto(json.dumps(payload).encode(), ('<broadcast>', PORT))

    def listener(self):
        while True:
            try:
                data, _ = self.recv_sock.recvfrom(4096)
                msg = json.loads(data.decode())
                sender = msg["router_name"]
                
                # 1. Bỏ qua gói tin của chính mình
                if sender == self.router_name: 
                    continue
                # 2. Bỏ qua gói tin nếu MÌNH ĐÃ CHẶN thằng gửi
                if sender in self.blocked_neighbors:
                    continue
                # 3. [FIX 2 CHIỀU] Bỏ qua gói tin nếu THẰNG GỬI ĐÃ CHẶN MÌNH
                if self.router_name in msg.get("blocked", []):
                    continue
                
                with self.lock:
                    if msg["type"] == "HELLO":
                        is_new = sender not in self.neighbors
                        self.neighbors[sender] = time.time()
                        if is_new: threading.Thread(target=self.trigger_update).start()
                            
                    elif msg["type"] == "UPDATE":
                        self.neighbors[sender] = time.time()
                        self.process_topology(sender, msg["routes"])
            except Exception: pass

    def process_topology(self, sender, received_routes):
        changed = False
        for net, info in received_routes.items():
            if net == self.local_net: continue
            if info.get("next_hop") == self.router_name: continue 
                
            rd = info["metric"]
            fd = rd + self.link_cost  
            
            if net not in self.topology_table: self.topology_table[net] = {}
            if sender not in self.topology_table[net] or self.topology_table[net][sender]["FD"] != fd:
                self.topology_table[net][sender] = {"RD": rd, "FD": fd}
                changed = True
                
        for net in list(self.topology_table.keys()):
            if net == self.local_net: continue
            if sender in self.topology_table[net] and net not in received_routes:
                del self.topology_table[net][sender]
                changed = True
                
        if changed: self.run_dual()

    def run_dual(self):
        route_changed = False
        for net, paths in list(self.topology_table.items()):
            if net == self.local_net: continue
            if not paths: 
                if net in self.routing_table:
                    del self.routing_table[net]
                    route_changed = True
                continue
                
            best_neighbor = min(paths, key=lambda k: paths[k]["FD"])
            best_fd = paths[best_neighbor]["FD"]
            
            if (net not in self.routing_table or 
                self.routing_table[net]["metric"] != best_fd or
                self.routing_table[net]["next_hop"] != best_neighbor):
                
                self.routing_table[net] = {"metric": best_fd, "next_hop": best_neighbor}
                route_changed = True

        if route_changed: threading.Thread(target=self.trigger_update).start()

    def check_timeouts(self):
        while True:
            time.sleep(1)
            current_time = time.time()
            with self.lock:
                dead_neighbors = [n for n, t in self.neighbors.items() if current_time - t > HOLD_TIME]
                for neighbor in dead_neighbors:
                    del self.neighbors[neighbor]
                    route_changed = False
                    for net in list(self.topology_table.keys()):
                        if neighbor in self.topology_table[net]:
                            del self.topology_table[net][neighbor]
                            route_changed = True
                    if route_changed: self.run_dual()

    def print_menu(self):
        print(f"\n=======================================================")
        print(f" ROUTER: {self.router_name} | MANG LAN: {self.local_net}")
        print(f" METRIC HIEN TAI TOI LANG GIENG: {self.link_cost}")
        print(f"=======================================================")
        print(" 1. Xem Bang Lang Gieng (Neighbor Table)")
        print(" 2. Xem Bang Cau Truc Mang (Topology Table - DUAL)")
        print(" 3. Xem Bang Dinh Tuyen (Routing Table)")
        print(" 4. Thay doi thong so (BW, Delay, Load, Rel)")
        print(" 5. Ngat hoac Ket noi lai Link voi lang gieng")
        print(" 0. Tat Nguon Router")
        print("=======================================================")

    def interactive_loop(self):
        threading.Thread(target=self.listener, daemon=True).start()
        threading.Thread(target=self.send_hello, daemon=True).start()
        threading.Thread(target=self.check_timeouts, daemon=True).start()
        
        while True:
            self.print_menu()
            choice = input("Chon chuc nang (0-5): ")
            
            with self.lock:
                if choice == '1':
                    print("\n[ BANG NEIGHBOR ]")
                    if not self.neighbors: print("  (Trong)")
                    for n, t in self.neighbors.items(): print(f"  - {n} (Hold: {round(time.time()-t, 1)}s)")
                
                elif choice == '2':
                    print("\n[ BANG TOPOLOGY (Phan tich DUAL) ]")
                    print(f"  {'Mang Dich':<15} | {'Di qua':<8} | {'RD':<10} | {'FD':<10} | {'Vai tro (Trang thai)'}")
                    print("  " + "-"*80)
                    
                    for net, paths in self.topology_table.items():
                        if net == self.local_net: continue
                        
                        best_neighbor = min(paths, key=lambda k: paths[k]["FD"])
                        best_fd = paths[best_neighbor]["FD"]
                        
                        for neighbor, metrics in paths.items():
                            rd = metrics['RD']
                            fd = metrics['FD']
                            
                            if neighbor == best_neighbor:
                                role = "[*] Successor (Duong chinh)"
                            elif rd < best_fd:
                                role = "[+] Feasible Successor (Du phong)"
                            else:
                                role = "[-] Backup (Nguy co Loop - Bi loai)"
                                
                            print(f"  {net:<15} | {neighbor:<8} | {rd:<10} | {fd:<10} | {role}")
                
                elif choice == '3':
                    print("\n[ BANG DINH TUYEN ]")
                    print(f"  {'Mang Dich':<15} | {'Metric (FD)':<12} | {'Next Hop'}")
                    for net, info in self.routing_table.items():
                        print(f"  {net:<15} | {info['metric']:<12} | {info['next_hop']}")
            
            if choice == '4':
                self.change_parameters()
            elif choice == '5':
                self.toggle_link()
            elif choice == '0':
                print("[!] Tat nguon Router...")
                sys.exit(0)
            
            if choice != '5': 
                input("\nNhan Enter de tiep tuc...")

if __name__ == "__main__":
    try:
        r_name = input("Ten Router (VD: R1): ")
        l_net = input("IP Mang LAN (VD: 192.168.1.0/24): ")
        router = InteractiveEIGRP(r_name, l_net)
        router.interactive_loop()
    except Exception as e:
        print("Loi:", e)
