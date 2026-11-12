#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif

using namespace std;

struct Result {
    size_t id;
    unsigned long long number;
    string factors;
};

struct NodeBuffer {
    queue<Result> data;
    mutex m;
    condition_variable canRead;
    condition_variable canWrite;
    bool finished = false;
};

struct Statistics {
    atomic<size_t> tasks{ 0 };
    atomic<size_t> readerWaits{ 0 };
    atomic<size_t> writerWaits{ 0 };
    atomic<size_t> portions{ 0 };
};

struct Delays {
    int calculation;
    int writing;
    string name;
};

string factorize(unsigned long long value) {
    if (value == 1) {
        return "(1)";
    }

    unsigned long long number = value;
    ostringstream result;

    for (unsigned long long divisor = 2; divisor <= number / divisor;) {
        int power = 0;
        while (number % divisor == 0) {
            number /= divisor;
            ++power;
        }

        if (power > 0) {
            result << '(' << divisor;
            if (power > 1) {
                result << "**" << power;
            }
            result << ')';
        }

        if (divisor == 2) {
            divisor = 3;
        }
        else {
            divisor += 2;
        }
    }

    if (number > 1) {
        result << '(' << number << ')';
    }

    return result.str();
}

void writePortion(int nodeNumber,
    vector<Result>& portion,
    ofstream& output,
    mutex& outputMutex,
    Statistics& statistics,
    int writingDelay) {
    this_thread::sleep_for(chrono::milliseconds(writingDelay));

    lock_guard<mutex> lock(outputMutex);
    output << "Узел " << nodeNumber << ", порция "
        << statistics.portions.fetch_add(1) + 1 << '\n';

    for (const Result& result : portion) {
        output << "Задача " << result.id << ": "
            << result.number << " = " << result.factors << '\n';
    }

    output << '\n';
    portion.clear();
}

void printWorkerFinished(const string& role, int nodeNumber) {
    static mutex consoleMutex;
    lock_guard<mutex> lock(consoleMutex);
    cout << role << " узла " << nodeNumber << " завершён" << '\n';
}

void readerWorker(int nodeNumber,
    NodeBuffer& buffer,
    ifstream& input,
    mutex& inputMutex,
    atomic<size_t>& nextId,
    Statistics& statistics,
    size_t queueCapacity,
    int calculationDelay) {
    while (true) {
        unsigned long long number = 0;
        size_t id = 0;

        {
            lock_guard<mutex> lock(inputMutex);
            if (!(input >> number)) {
                break;
            }
            id = nextId.fetch_add(1);
        }

        string factors = factorize(number);
        this_thread::sleep_for(chrono::milliseconds(calculationDelay));

        unique_lock<mutex> lock(buffer.m);
        if (buffer.data.size() >= queueCapacity) {
            statistics.readerWaits.fetch_add(1);
        }

        buffer.canWrite.wait(lock, [&buffer, queueCapacity] {
            return buffer.data.size() < queueCapacity;
            });

        buffer.data.push({ id, number, factors });
        statistics.tasks.fetch_add(1);
        lock.unlock();
        buffer.canRead.notify_one();
    }

    {
        lock_guard<mutex> lock(buffer.m);
        buffer.finished = true;
    }
    buffer.canRead.notify_all();

    printWorkerFinished("Вычислитель", nodeNumber);
}

void writerWorker(int nodeNumber,
    NodeBuffer& buffer,
    ofstream& output,
    mutex& outputMutex,
    Statistics& statistics,
    size_t portionSize,
    int writingDelay) {
    vector<Result> portion;
    portion.reserve(portionSize);

    while (true) {
        unique_lock<mutex> lock(buffer.m);

        if (buffer.data.empty() && !buffer.finished) {
            statistics.writerWaits.fetch_add(1);
        }

        buffer.canRead.wait(lock, [&buffer] {
            return !buffer.data.empty() || buffer.finished;
            });

        if (buffer.data.empty() && buffer.finished) {
            break;
        }

        Result result = move(buffer.data.front());
        buffer.data.pop();
        lock.unlock();
        buffer.canWrite.notify_one();

        portion.push_back(move(result));

        if (portion.size() == portionSize) {
            writePortion(nodeNumber, portion, output, outputMutex,
                statistics, writingDelay);
        }
    }

    if (!portion.empty()) {
        writePortion(nodeNumber, portion, output, outputMutex,
            statistics, writingDelay);
    }

    printWorkerFinished("Писатель", nodeNumber);
}

bool runSituation(int situationNumber,
    const Delays& delays,
    size_t nodeCount,
    size_t queueCapacity,
    size_t portionSize) {
    ifstream input("input.txt");
    if (!input.is_open()) {
        cout << "Не удалось открыть input.txt" << '\n';
        return false;
    }

    string outputName = "output_" + to_string(situationNumber) + ".txt";
    ofstream output(outputName);
    if (!output.is_open()) {
        cout << "Не удалось создать " << outputName << '\n';
        return false;
    }

    output << delays.name << '\n';
    output << "Узлов: " << nodeCount
        << ", ёмкость буфера: " << queueCapacity
        << ", размер порции: " << portionSize << "\n\n";

    mutex inputMutex;
    mutex outputMutex;
    atomic<size_t> nextId{ 1 };
    Statistics statistics;

    vector<unique_ptr<NodeBuffer>> buffers;
    vector<thread> threads;
    buffers.reserve(nodeCount);
    threads.reserve(nodeCount * 2);

    for (size_t i = 0; i < nodeCount; ++i) {
        buffers.push_back(make_unique<NodeBuffer>());
    }

    cout << '\n' << delays.name << '\n';
    cout << "Задержка вычислителя: " << delays.calculation << " мс" << '\n';
    cout << "Задержка писателя: " << delays.writing << " мс" << '\n';

    auto start = chrono::steady_clock::now();

    for (size_t i = 0; i < nodeCount; ++i) {
        threads.emplace_back(readerWorker,
            static_cast<int>(i + 1),
            ref(*buffers[i]),
            ref(input),
            ref(inputMutex),
            ref(nextId),
            ref(statistics),
            queueCapacity,
            delays.calculation);

        threads.emplace_back(writerWorker,
            static_cast<int>(i + 1),
            ref(*buffers[i]),
            ref(output),
            ref(outputMutex),
            ref(statistics),
            portionSize,
            delays.writing);
    }

    for (thread& worker : threads) {
        worker.join();
    }

    auto finish = chrono::steady_clock::now();
    double seconds = chrono::duration<double>(finish - start).count();

    input.close();
    output.close();

    cout << "Обработано задач: " << statistics.tasks.load() << '\n';
    cout << "Ожиданий вычислителей: " << statistics.readerWaits.load() << '\n';
    cout << "Ожиданий писателей: " << statistics.writerWaits.load() << '\n';
    cout << "Записано порций: " << statistics.portions.load() << '\n';
    cout << fixed << setprecision(3)
        << "Время выполнения: " << seconds << " с" << '\n';
    cout << "Результаты: " << outputName << '\n';

    return true;
}

size_t readPositive(const string& message) {
    while (true) {
        cout << message;
        long long value = 0;
        if (cin >> value && value > 0) {
            return static_cast<size_t>(value);
        }
        cin.clear();
        cin.ignore(10000, '\n');
        cout << "Введите положительное число" << '\n';
    }
}

int main() {
#ifdef _WIN32
    SetConsoleCP(1251);
    SetConsoleOutputCP(1251);
#endif

    cout << "Лабораторная работа 2" << '\n';
    cout << "Модель рпк2, задача рм" << "\n\n";

    size_t nodeCount = readPositive("Количество узлов N: ");
    size_t queueCapacity = readPositive("Ёмкость буфера K: ");
    size_t portionSize = readPositive("Размер порции B: ");

    vector<Delays> situations = {
        {40, 40, "Ситуация 1. Одинаковая скорость потоков"},
        {10, 120, "Ситуация 2. Вычислители быстрее писателей"},
        {120, 10, "Ситуация 3. Писатели быстрее вычислителей"}
    };

    for (size_t i = 0; i < situations.size(); ++i) {
        if (!runSituation(static_cast<int>(i + 1), situations[i],
            nodeCount, queueCapacity, portionSize)) {
            return 1;
        }
    }

    cout << "\nВсе ситуации выполнены" << '\n';
    return 0;
}
