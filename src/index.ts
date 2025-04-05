import { KafkaExpressApp } from "./app.js";
import { KafkaTaskService } from "./KafkaTaskService.js";

const port = Number(process.env.PORT) || 3000;
const kafkaExpressApp = new KafkaExpressApp(port, new KafkaTaskService());
kafkaExpressApp.start();
