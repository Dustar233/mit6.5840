import subprocess
import concurrent.futures
import time

# === 配置区 ===
TEST_REGEX = "3C"  # 你可以改成单独的 "3A" 或 "3A|3B" 等
ITERATIONS = 100         # 测试总次数
WORKERS = 16              # 并发数（建议设置为你的 CPU 逻辑核心数的一半）
TIMEOUT = 300            # 单次测试的超时时间（秒）。Lab 3 完整跑完通常需要 2-3 分钟，设置 300 比较安全
# ============

def run_test(task_id):
    # -race 开启竞争检测，对于分布式系统调试必不可少
    cmd = f"go test -run '{TEST_REGEX}' -race"
    start_time = time.time()
    
    try:
        # 执行命令，捕获 stdout 和 stderr
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=TIMEOUT)
        duration = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ [Pass] Task {task_id:03d} ({duration:.1f}s)")
            return True
        else:
            print(f"❌ [FAIL] Task {task_id:03d} ({duration:.1f}s) - 保存日志...")
            log_file = f"logs/fail_Task{task_id:03d}.log"
            with open(log_file, "w") as f:
                f.write(result.stdout)
                f.write(result.stderr)
            return False
            
    except subprocess.TimeoutExpired as e:
        print(f"⚠️ [TIMEOUT] Task {task_id:03d} (> {TIMEOUT}s) - 可能是死锁，保存日志...")
        log_file = f"logs/timeout_Task{task_id:03d}.log"
        with open(log_file, "w") as f:
            f.write(f"测试超时，设定时间为 {TIMEOUT} 秒。\n")
            # Python 的超时异常包含标准输出字节流
            if e.stdout: f.write(e.stdout.decode('utf-8', errors='ignore'))
            if e.stderr: f.write(e.stderr.decode('utf-8', errors='ignore'))
        return False

if __name__ == "__main__":
    print(f"🚀 开始并发测试: 目标 {TEST_REGEX}")
    print(f"总计 {ITERATIONS} 次, 并发数 {WORKERS}, 预计时间取决于最慢的测试...")
    start_all = time.time()
    
    passed = 0
    failed = 0
    
    # 使用线程池并发执行测试
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(run_test, i) for i in range(1, ITERATIONS + 1)]
        
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                passed += 1
            else:
                failed += 1
                
    print("-" * 40)
    print(f"📊 测试完成! 总耗时: {time.time() - start_all:.1f}s")
    print(f"✅ 成功: {passed} | ❌ 失败/超时: {failed}")
    if failed > 0:
        print("📝 请检查当前目录下的 fail_*.log 或 timeout_*.log 了解详细报错。")
