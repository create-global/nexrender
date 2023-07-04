const { send }   = require('micro')
const { fetch }  = require('../helpers/database')
const { update } = require('../helpers/database')
const { Mutex }  = require('async-mutex');

const mutex = new Mutex();

let lastJobStage = null;
module.exports = async (req, res) => {
    const release = await mutex.acquire();

    try{
        console.log(`fetching a pickup job for a worker`)

        // Default to 'default' jobs for backwards compatibility
        const types = req.query.types ? JSON.parse(req.query.types) : [{ type: 'default '}]

        const listing = await fetch(null,types)
        const queued  = listing.filter(job => job.state == 'queued')

        if (queued.length < 1) {
            return send(res, 200, {})
        }

        let job;

        if (process.env.NEXRENDER_ORDERING == 'random') {
            job = queued[Math.floor(Math.random() * queued.length)];
        }
        else if (process.env.NEXRENDER_ORDERING == 'newest-first') {
            job = queued[queued.length-1];
        } else if (process.env.NEXRENDER_ORDERING == 'priority') {
            // Get the job with the largest priority number
            // This will also sort them by the date, so if 2 jobs have the same
            // priority, it will choose the oldest one because that's the original state
            // of the array in question
            job = queued.sort((a, b) => {
                // Quick sanitisation to make sure they're numbers
                if (isNaN(a.priority)) a.priority = 0
                if (isNaN(b.priority)) b.priority = 0
                return b.priority - a.priority
            })[0]
        } else if (process.env.NEXRENDER_ORDERING === 'stage-distributed') {
            // collect all possible stages
            const stages = types.reduce((res, item) => {
                const jobStages = item.filterPolicy?.stage;
                if (!Array.isArray(jobStages)) {
                    return res;
                }
                jobStages.forEach(stage => {
                    if (!res.includes(stage)) {
                        res.push(stage)
                    }
                })

                return res;
            }, [])

            if (!stages.length || !lastJobStage) {
                return queued[0];
            }

            job = queued.sort((a, b) => {
                const aStageIndex = stages.indexOf(a.ct?.attributes?.stage || '')
                const bStageIndex = stages.indexOf(b.ct?.attributes?.stage || '')
                if (aStageIndex === bStageIndex) {
                    return new Date(a.createdAt).getTime() > new Date(b.createdAt).getTime() ? 1 : -1
                }
                return aStageIndex - bStageIndex
            })[0]
            lastJobStage = job.ct?.attributes?.stage;
        }
        else { /* fifo (oldest-first) */
            job = queued[0];
        }

        /* update the job locally, and send it to the worker */
        send(res, 200, await update(
            job.uid,
            { state: 'picked', executor: req.headers["x-forwarded-for"] || req.socket.remoteAddress },
            { transaction: true }
        ))
    } finally {
        release();
    }
}
