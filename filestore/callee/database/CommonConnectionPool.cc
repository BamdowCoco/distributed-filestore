#include "CommonConnectionPool.hpp"
#include "mprpc_config.h"
#include "logger.h"

#include <thread>
#include <chrono>

ConnectionPool::ConnectionPool()
    : _port(0),
      _initSize(0),
      _maxSize(0),
      _maxIdleTime(0),
      _connectionTimeout(0),
      _connectionCount(0)
{
    if (!loadConfigFile()) {
        LOG_ERROR("load mysql config file failed!");
        return;
    }

    // 确保目标数据库存在（不存在则自动创建）
    if (!ensureDatabaseExists()) {
        LOG_ERROR("ensure database exists failed!");
        return;
    }

    // 预创建初始连接
    for (int i = 0; i < _initSize; ++i) {
        Connection* conn = new Connection();
        if (conn->connect(_ip, _port, _username, _password, _dbname)) {
            conn->refreshIdleStart();
            _connectionQue.push(conn);
            _connectionCount++;
        } else {
            delete conn;
        }
    }

    // 启动生产线程与定时回收线程
    std::thread producer(&ConnectionPool::produceConnectionTask, this);
    producer.detach();
    std::thread scanner(&ConnectionPool::scanConnectionTask, this);
    scanner.detach();
}

// 注意：按 getInstance 里的说明，这个单例在进程退出时**不会被析构**，
// 因此本函数实际上不会被执行（保留它是为了表达"池拥有这些连接"的语义）。
ConnectionPool::~ConnectionPool()
{
    // 释放队列中残留连接
    while (!_connectionQue.empty()) {
        Connection* conn = _connectionQue.front();
        _connectionQue.pop();
        delete conn;
    }
}

ConnectionPool& ConnectionPool::getInstance()
{
    // 有意**不析构**这个单例：池内那两个 detach 的后台线程会一直等在没有停止谓词的
    // _cvProducer/_cvConsumer 上，若在进程退出时析构单例，成员条件变量的
    // pthread_cond_destroy 会等到等待者离开——而它们永远不会离开，于是 exit() 永久挂住。
    // 连接池是进程级生命周期对象，退出时交给 OS 回收即可（这也是带后台线程的单例常见做法）。
    //
    // 这条路径只在 exit() 时才会走到，且此前一直没被触发：直到元数据服务「缺票据私钥就
    // 拒绝启动」的 fail-closed 在构造函数里调用 exit()（见 meta_service.cc）。
    static ConnectionPool* pool = new ConnectionPool();
    return *pool;
}

std::shared_ptr<Connection> ConnectionPool::getConnection()
{
    std::unique_lock<std::mutex> lock(_queueMutex);
    while (_connectionQue.empty()) {
        // 队列为空，等待生产者放入连接，超时则返回 nullptr
        if (std::cv_status::timeout ==
            _cvConsumer.wait_for(lock, std::chrono::milliseconds(_connectionTimeout))) {
            if (_connectionQue.empty()) {
                LOG_ERROR("get connection timeout!");
                return nullptr;
            }
        }
    }

    Connection* conn = _connectionQue.front();
    _connectionQue.pop();
    // 通知生产者线程继续生产
    _cvProducer.notify_all();

    // 返回智能指针，自定义删除器把连接归还队列
    return std::shared_ptr<Connection>(conn, [this](Connection* c) {
        std::unique_lock<std::mutex> l(_queueMutex);
        c->refreshIdleStart();
        _connectionQue.push(c);
        _cvConsumer.notify_all();
    });
}

bool ConnectionPool::loadConfigFile()
{
    MprpcConfig cfg;
    if (!cfg.loadConfigFile("config/mysql.cnf")) {
        LOG_ERROR("load config/mysql.cnf failed!");
        return false;
    }

    _ip = cfg.load("ip");
    _port = static_cast<unsigned short>(std::stoi(cfg.load("port")));
    _username = cfg.load("username");
    _password = cfg.load("password");
    _dbname = cfg.load("dbname");
    _initSize = std::stoi(cfg.load("init_size"));
    _maxSize = std::stoi(cfg.load("max_size"));
    _maxIdleTime = std::stoi(cfg.load("max_idle_time"));
    _connectionTimeout = std::stoi(cfg.load("connection_timeout"));

    return true;
}

bool ConnectionPool::ensureDatabaseExists()
{
    // 先不带库名连接，仅用于建库（指定了不存在的库名会导致连接失败）
    MYSQL* conn = mysql_init(nullptr);
    if (conn == nullptr) {
        return false;
    }
    MYSQL* ret = mysql_real_connect(conn, _ip.c_str(), _username.c_str(), _password.c_str(),
                                    nullptr, _port, nullptr, 0);
    if (ret == nullptr) {
        LOG_ERROR("connect mysql (no db) failed! error:%s", mysql_error(conn));
        mysql_close(conn);
        return false;
    }

    std::string sql = "CREATE DATABASE IF NOT EXISTS `" + _dbname +
                      "` DEFAULT CHARACTER SET utf8mb4";
    int rc = mysql_query(conn, sql.c_str());
    if (rc != 0) {
        LOG_ERROR("create database failed! db:%s error:%s", _dbname.c_str(), mysql_error(conn));
    }
    mysql_close(conn);
    return rc == 0;
}

void ConnectionPool::produceConnectionTask()
{
    while (!_isStop.load()) {
        std::unique_lock<std::mutex> lock(_queueMutex);
        // 队列非空时无需生产，等待消费者取走连接
        while (!_connectionQue.empty() && !_isStop.load()) {
            _cvProducer.wait(lock);
        }
        if (_isStop.load()) {
            break;
        }
        // 达到最大连接数则等待
        if (_connectionCount >= _maxSize) {
            _cvProducer.wait(lock);
            continue;
        }

        Connection* conn = new Connection();
        if (conn->connect(_ip, _port, _username, _password, _dbname)) {
            conn->refreshIdleStart();
            _connectionQue.push(conn);
            _connectionCount++;
            _cvConsumer.notify_all();
        } else {
            delete conn;
        }
    }
}

void ConnectionPool::scanConnectionTask()
{
    while (!_isStop.load()) {
        // 定时扫描（每秒），回收空闲超时的连接
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (_isStop.load()) {
            break;
        }

        std::unique_lock<std::mutex> lock(_queueMutex);
        // 保留至少 _initSize 个连接，队头空闲时间最长
        while (_connectionQue.size() > static_cast<size_t>(_initSize)) {
            Connection* conn = _connectionQue.front();
            if (conn->getIdleAliveDuration() >= _maxIdleTime) {
                _connectionQue.pop();
                delete conn;
                _connectionCount--;
            } else {
                // 队头连接未超时，后续连接空闲时间更短，直接退出
                break;
            }
        }
    }
}

void ConnectionPool::stop()
{
    _isStop.store(true);
    _cvProducer.notify_all();
    _cvConsumer.notify_all();
}
