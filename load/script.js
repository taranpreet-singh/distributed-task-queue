import http from "k6/http";
import { check } from "k6";

// Target the API by its compose service name when run as a container,
// or override with BASE_URL=http://localhost:8090 when run from the host.
const BASE_URL = __ENV.BASE_URL || "http://api:8090";

const recipients = [
  "alice@example.com",
  "bob@example.com",
  "charlie@example.com",
];
const subjects = ["Welcome", "Password reset", "Weekly digest", "Order placed"];

function rand(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function emailTask() {
  return {
    type: "SendEmail",
    payload: {
      to: rand(recipients),
      subject: rand(subjects),
      body: "Load test task from k6",
    },
  };
}

function flakyTask() {
  return {
    type: "FlakyTask",
    payload: { fail_rate: 0.5 },
  };
}

// noHandlerTask uses a task type the worker has no handler for. It enqueues
// fine (202) but is dead-lettered immediately with reason "no_handler" —
// no retries, unlike FlakyTask.
function noHandlerTask() {
  return {
    type: "UnknownTask",
    payload: {},
  };
}

export const options = {
  // Which stats to print for trend metrics (e.g. http_req_duration) in the
  // end-of-test summary. p(99) is not shown by default.
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  stages: [
    { duration: "30s", target: 50 }, // ramp up to 50 virtual users
    { duration: "1m", target: 50 }, // hold at 50
    { duration: "30s", target: 0 }, // ramp back down
  ],
  // Pass/fail criteria for the whole run. If any threshold is breached,
  // k6 exits with a non-zero code — the CI "gate".
  thresholds: {
    http_req_duration: ["p(95)<50"], // 95% of requests must finish under 50ms
    http_req_failed: ["rate<0.01"], // fewer than 1% of requests may fail
    checks: ["rate>0.99"], // more than 99% of checks must pass
  },
};

export default function () {
  // Weighted mix of task types so the load exercises every worker path:
  //   ~50% SendEmail    — succeed
  //   ~35% FlakyTask    — retry, some land in DLQ as "max_retries"
  //   ~15% UnknownTask  — dead-lettered immediately as "no_handler"
  const r = Math.random();
  let task;
  if (r < 0.5) {
    task = emailTask();
  } else if (r < 0.85) {
    task = flakyTask();
  } else {
    task = noHandlerTask();
  }
  const payload = JSON.stringify(task);

  const res = http.post(`${BASE_URL}/tasks`, payload, {
    headers: { "Content-Type": "application/json" },
  });

  check(res, {
    "status is 202": (r) => r.status === 202,
  });
}
