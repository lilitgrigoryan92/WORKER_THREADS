const path = require("path");
const os = require("os");
const fs = require("fs");
const csv = require("csv-parser");
const { writeFile, mkdirSync, existsSync } = require("fs");
const { Worker, isMainThread } = require("worker_threads");
 const { parentPort } = require("worker_threads");


const num=os.cpus().length
class Convert {
  constructor(dirPath) {
    this.dirPath = dirPath;
    this.workerCount = Math.min(10, num);
  }

  readFilesCsv() {
    if (!this.dirPath) {
      throw new Error("no directory");
    }

    return new Promise((resolve, reject) => {
      fs.readdir(this.dirPath, (err, files) => {
        if (err) {
          reject(err);
          return;
        }

        const csvFiles = files.filter((file) => file.endsWith(".csv"));

        if (!csvFiles) {
          reject(new Error("  no CSV files"));
          return;
        }

        const conv = path.join(__dirname, "converted");
        if (!existsSync(conv)) {
          mkdirSync(conv);
        }

        const start = Date.now();
        let count = 0;

        const workers = csvFiles.map((file) => {
          const input = path.join(this.dirPath, file);
          const output = path.join(conv, file.replace(".csv", ".json"));

          return new Promise((resolve, reject) => {
            const worker = new Worker(__filename);

            worker.on("message", (message) => {
              count += message.count;
              worker.terminate();

              resolve();
            });

            worker.on("error", (error) => {
              reject(error);
            });


            worker.postMessage({ input, output });
          });
        });

        Promise.all(workers)
          .then(() => {
            const end = Date.now();
            const duration = end - start;
            resolve({ count, duration });
          })
          .catch((error) => {
            reject(error);
          });
      });
    });
  }
}

if (isMainThread) {
  const dirPath = process.argv[2];
  const instance = new Convert(dirPath);


  instance
  .readFilesCsv()
    .then(({ count, duration }) => {
      console.log(count);
      console.log(duration,"ms");
    })
    .catch((error) => console.log(error));
} else {

  parentPort.on("message", ({ input, output }) => {
    const results = [];

    fs.createReadStream(input)
      .pipe(csv())
      .on("data", (data) => {
        results.push(data);
      })
      .on("end", () => {
        writeFile(output, JSON.stringify(results, undefined, 2), (error) => {
          if (error) throw error;
          parentPort.postMessage({ count: results.length });
        });
      })
      })
    }

