const { send }   = require('micro')
const { fetch, update } = require('../helpers/database')
const { filterAndSortJobs } = require('@create-global/nexrender-core')
const { Mutex }  = require('async-mutex');

const mutex = new Mutex();

const concurrencyLimits = process.env.NEXRENDER_CONCURRENCY_LIMITS
    ? JSON.parse(process.env.NEXRENDER_CONCURRENCY_LIMITS)
    : {};

const hasConcurrencyLimits = Object.keys(concurrencyLimits).length > 0;

const isInProgress = (state) =>
    state === 'picked' || state === 'started' || (state && state.startsWith('render:'));

module.exports = async (req, res) => {
    const release = await mutex.acquire();

    try{
        console.log(`fetching a pickup job for a worker`)

        // Default to 'default' jobs for backwards compatibility
        const types = req.query.types ? JSON.parse(req.query.types) : [{ type: 'default'}]

        // Fetch all jobs of the requested types (type-only, no filterPolicy)
        // so we can count in-progress jobs accurately for concurrency checks
        const jobs = await fetch(null, types.map(t => ({ type: t.type })))

        // Check concurrency limits: exclude types that have reached their max in-progress count
        const excludedTypes = new Set();
        if (hasConcurrencyLimits) {
            const inProgressCounts = {};
            for (const job of jobs) {
                if (isInProgress(job.state)) {
                    const type = job.type || 'default';
                    inProgressCounts[type] = (inProgressCounts[type] || 0) + 1;
                }
            }
            for (const [type, limit] of Object.entries(concurrencyLimits)) {
                if ((inProgressCounts[type] || 0) >= limit) {
                    excludedTypes.add(type);
                }
            }
        }

        // Filter for queued jobs, exclude concurrency-exceeded types, then apply filterPolicy
        const queuedJobs = jobs.filter(job => job.state == 'queued' && !excludedTypes.has(job.type))
        const candidates = filterAndSortJobs(queuedJobs, types)

        if (candidates.length < 1) {
            return send(res, 200, {})
        }

        let job;

        if (process.env.NEXRENDER_ORDERING == 'random') {
            job = candidates[Math.floor(Math.random() * candidates.length)];
        }
        else if (process.env.NEXRENDER_ORDERING == 'newest-first') {
            job = candidates[candidates.length-1];
        } else if (process.env.NEXRENDER_ORDERING == 'priority') {
            // Get the job with the largest priority number
            // This will also sort them by the date, so if 2 jobs have the same
            // priority, it will choose the oldest one because that's the original state
            // of the array in question
            job = candidates.sort((a, b) => {
                // Quick sanitisation to make sure they're numbers
                if (isNaN(a.priority)) a.priority = 0
                if (isNaN(b.priority)) b.priority = 0
                return b.priority - a.priority
            })[0]
        } else if (process.env.NEXRENDER_ORDERING === 'stage-distributed') {
            const jobs = Object.values(candidates.reduce((res, item) => {
                const stage = item.ct?.attributes?.stage || 'no-stage';
                if (!res[stage]) {
                    return {
                        ...res,
                        [stage]: item
                    };
                }
                return res;
            }, {}))

            // pick random
            job = jobs[Math.floor(Math.random() * jobs.length)];
        }
        else { /* fifo (oldest-first) */
            job = candidates[0];
        }

        /* update the job locally, and send it to the worker */
        const pickedJob = await update(
            job.uid,
            { state: 'picked', executor: req.headers['x-worker-name'] || req.headers["x-forwarded-for"] || req.socket.remoteAddress },
            { transaction: true }
        );

        // Hook: allows modifying job before sending to worker (e.g. inject credentials)
        if (req.onJobPickup && typeof req.onJobPickup === 'function') {
            try {
                await req.onJobPickup(pickedJob);
            } catch (err) {
                console.error('[pickup] Failed to run onJobPickup hook:', err.message);
            }
        }

        send(res, 200, pickedJob)
    } finally {
        release();
    }
}
