const { Queue } = require("bullmq");

// List of microservices
const MicroServices = ["M-1", "M-2", "M-3", "M-4"];

// Redis connection configuration
const REDIS_CONNECTION_OPTIONS = {
	port: parseInt(process.env.REDIS_PORT || "6379"),
	host: process.env.REDIS_HOST || "127.0.0.1",
	username: "default",
	password: process.env.REDIS_PASSWORD || "",
	retryStrategy: (attempts) => {
		console.log(`Attempt #${attempts}: Trying to reconnect to Redis...`);
		if (attempts > 5) {
			console.error("Maximum reconnection attempts reached. Shutting down...");
			process.exit(1); // Exit the process after 5 failed attempts
		}
		return Math.min(attempts * 100, 3000); // Gradually increase retry delay
	},
};

class AutoScalingLoadBalancer {
	constructor() {
		this.queues = {};
		this.setupQueues();
	}

	/**
	 * Initializes a queue for each microservice.
	 */
	setupQueues() {
		for (const service of MicroServices) {
			try {
				this.queues[service] = new Queue(`MQueue-${service}`, {
					connection: REDIS_CONNECTION_OPTIONS,
				});
				console.log(`Queue created for ${service}`);
			} catch (error) {
				console.error(`Failed to create queue for ${service}: ${error.message}`);
			}
		}
	}

	/**
	 * Distributes incoming jobs to the least busy queue.
	 * @param {Object} jobData - Data for the incoming job.
	 * @param {number} [priority=2] - Priority level (1 = high, 2 = normal, 3 = low).
	 */
	async distributeJob(jobData, priority = 2) {
		const selectedQueue = await this.findOptimalQueue();

		if (selectedQueue) {
			try {
				await selectedQueue.add("PlaceOrderJob", jobData, {
					priority: this.getPriorityValue(priority),
				});
				console.log(`Job sent to ${selectedQueue.name} with priority ${priority}`);
			} catch (error) {
				console.error("Failed to distribute the job:", error.message);
			}
		} else {
			console.error("No available queues to handle the job.");
		}
	}

	// /**
	//  * Converts priority level to a corresponding value in BullMQ.
	//  * @param {number} priority - Priority level (1, 2, or 3).
	//  * @returns {number} Corresponding BullMQ priority value.
	//  */
	// getPriorityValue(priority) {
	// 	const priorityMap = {
	// 		1: 1, // High Priority
	// 		2: 50, // Normal Priority
	// 		3: 100, // Low Priority
	// 	};
	// 	return priorityMap[priority] || 50; // Default to normal priority
	// }

	/**
	 * Finds the queue with the least waiting jobs.
	//  * @returns {Promise<Queue | null>} The best available queue or null.
	//  */
	// async findOptimalQueue() {
	// 	const healthyQueues = await this.getHealthyQueues();
	// 	if (healthyQueues.length === 0) {
	// 		return null;
	// 	}
	// 	return this.selectQueueWithLeastJobs(healthyQueues);
	// }

	/**
	 * Returns only the healthy queues.
	 * @returns {Promise<Queue[]>} Array of healthy queues.
	 */
	// async getHealthyQueues() {
	// 	const queueNames = Object.keys(this.queues);
	// 	const healthyQueues = await Promise.all(
	// 		queueNames.map(async (queueName) => {
	// 			const queue = this.queues[queueName];
	// 			return (await this.isQueueHealthy(queue)) ? queue : null;
	// 		})
	// 	);

	// 	return healthyQueues.filter((queue) => queue !== null);
	// }

	/**
	 * Checks the health of a given queue.
	 * @param {Queue} queue - The queue to check.
	 * @returns {Promise<boolean>} True if the queue is healthy, false otherwise.
	 */
	async isQueueHealthy(queue) {
		try {
			const waitingCount = await queue.getWaitingCount();
			const isHealthy = waitingCount < 1000; // Mark queue as unhealthy if >1000 jobs

			if (!isHealthy) {
				console.warn(`Queue ${queue.name} is overloaded.`);
			}
			return isHealthy;
		} catch (error) {
			console.error(`Error checking queue ${queue.name}: ${error.message}`);
			return false;
		}
	}

	/**
	 * Selects the queue with the least number of waiting jobs.
	 * @param {Queue[]} queues - List of healthy queues.
	 * @returns {Promise<Queue | null>} The selected queue or null.
	 */
	async selectQueueWithLeastJobs(queues) {
		if (queues.length === 0) {
			return null;
		}

		const queueStats = await Promise.all(
			queues.map(async (queue) => ({
				queue,
				waitingJobs: await queue.getWaitingCount(),
			}))
		);

		// Sort queues by waiting job count (ascending)
		queueStats.sort((a, b) => a.waitingJobs - b.waitingJobs);

		// Get queues with the fewest waiting jobs
		const minJobs = queueStats[0].waitingJobs;
		const leastBusyQueues = queueStats.filter(
			(q) => q.waitingJobs === minJobs
		);

		// Randomly select one of the least busy queues
		const selectedQueue =
			leastBusyQueues[Math.floor(Math.random() * leastBusyQueues.length)].queue;

		return selectedQueue;
	}
}

// Create and export the load balancer
const autoScalingLoadBalancer = new AutoScalingLoadBalancer();
module.exports = { autoScalingLoadBalancer };
