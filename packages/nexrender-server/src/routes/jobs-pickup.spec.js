const originalEnv = process.env.NEXRENDER_CONCURRENCY_LIMITS;

let mockData = [];

jest.mock('../helpers/database', () => {
    const { filterAndSortJobs } = require('@create-global/nexrender-core');
    return {
        fetch: jest.fn((uid, types = []) => {
            if (uid) return mockData.find(j => j.uid === uid);
            return filterAndSortJobs(mockData, types);
        }),
        update: jest.fn((uid, updates) => {
            const job = mockData.find(j => j.uid === uid);
            return { ...job, ...updates };
        }),
    };
});

const createReq = (types) => ({
    query: types ? { types: JSON.stringify(types) } : {},
    headers: { 'x-worker-name': 'test-worker' },
    socket: { remoteAddress: '127.0.0.1' },
});

const createRes = () => ({});

let capturedStatus, capturedBody;
jest.mock('micro', () => ({
    send: jest.fn((res, status, body) => {
        capturedStatus = status;
        capturedBody = body;
    }),
}));

describe('jobs-pickup with concurrency limits', () => {
    beforeEach(() => {
        mockData = [];
        capturedStatus = null;
        capturedBody = null;
        jest.clearAllMocks();
    });

    describe('when NEXRENDER_CONCURRENCY_LIMITS is set', () => {
        afterAll(() => {
            if (originalEnv !== undefined) {
                process.env.NEXRENDER_CONCURRENCY_LIMITS = originalEnv;
            } else {
                delete process.env.NEXRENDER_CONCURRENCY_LIMITS;
            }
            jest.resetModules();
        });

        it('should not pick up a figma job when one is already picked', async () => {
            process.env.NEXRENDER_CONCURRENCY_LIMITS = JSON.stringify({ figma: 1 });
            jest.resetModules();

            mockData = [
                { uid: '1', type: 'figma', state: 'picked', createdAt: new Date('2024-01-01') },
                { uid: '2', type: 'figma', state: 'queued', createdAt: new Date('2024-01-02') },
            ];

            const handler = require('./jobs-pickup');
            await handler(createReq([{ type: 'figma' }]), createRes());

            expect(capturedStatus).toBe(200);
            expect(capturedBody).toEqual({});
        });

        it('should not pick up a figma job when one is in started state', async () => {
            process.env.NEXRENDER_CONCURRENCY_LIMITS = JSON.stringify({ figma: 1 });
            jest.resetModules();

            mockData = [
                { uid: '1', type: 'figma', state: 'started', createdAt: new Date('2024-01-01') },
                { uid: '2', type: 'figma', state: 'queued', createdAt: new Date('2024-01-02') },
            ];

            const handler = require('./jobs-pickup');
            await handler(createReq([{ type: 'figma' }]), createRes());

            expect(capturedStatus).toBe(200);
            expect(capturedBody).toEqual({});
        });

        it('should not pick up a figma job when one is in render:* state', async () => {
            process.env.NEXRENDER_CONCURRENCY_LIMITS = JSON.stringify({ figma: 1 });
            jest.resetModules();

            mockData = [
                { uid: '1', type: 'figma', state: 'render:dorender', createdAt: new Date('2024-01-01') },
                { uid: '2', type: 'figma', state: 'queued', createdAt: new Date('2024-01-02') },
            ];

            const handler = require('./jobs-pickup');
            await handler(createReq([{ type: 'figma' }]), createRes());

            expect(capturedStatus).toBe(200);
            expect(capturedBody).toEqual({});
        });

        it('should pick up a figma job when none are in progress', async () => {
            process.env.NEXRENDER_CONCURRENCY_LIMITS = JSON.stringify({ figma: 1 });
            jest.resetModules();

            mockData = [
                { uid: '1', type: 'figma', state: 'finished', createdAt: new Date('2024-01-01') },
                { uid: '2', type: 'figma', state: 'queued', createdAt: new Date('2024-01-02') },
            ];

            const handler = require('./jobs-pickup');
            await handler(createReq([{ type: 'figma' }]), createRes());

            expect(capturedStatus).toBe(200);
            expect(capturedBody).toEqual(expect.objectContaining({ uid: '2', state: 'picked' }));
        });

        it('should still pick up jobs of other types when figma is at limit', async () => {
            process.env.NEXRENDER_CONCURRENCY_LIMITS = JSON.stringify({ figma: 1 });
            jest.resetModules();

            mockData = [
                { uid: '1', type: 'figma', state: 'started', createdAt: new Date('2024-01-01') },
                { uid: '2', type: 'figma', state: 'queued', createdAt: new Date('2024-01-02') },
                { uid: '3', type: 'default', state: 'queued', createdAt: new Date('2024-01-03') },
            ];

            const handler = require('./jobs-pickup');
            await handler(createReq([{ type: 'figma' }, { type: 'default' }]), createRes());

            expect(capturedStatus).toBe(200);
            expect(capturedBody).toEqual(expect.objectContaining({ uid: '3', state: 'picked' }));
        });

        it('should allow up to maxConcurrency jobs in progress', async () => {
            process.env.NEXRENDER_CONCURRENCY_LIMITS = JSON.stringify({ figma: 2 });
            jest.resetModules();

            mockData = [
                { uid: '1', type: 'figma', state: 'started', createdAt: new Date('2024-01-01') },
                { uid: '2', type: 'figma', state: 'queued', createdAt: new Date('2024-01-02') },
            ];

            const handler = require('./jobs-pickup');
            await handler(createReq([{ type: 'figma' }]), createRes());

            expect(capturedStatus).toBe(200);
            expect(capturedBody).toEqual(expect.objectContaining({ uid: '2', state: 'picked' }));
        });

        it('should block when maxConcurrency of 2 is reached', async () => {
            process.env.NEXRENDER_CONCURRENCY_LIMITS = JSON.stringify({ figma: 2 });
            jest.resetModules();

            mockData = [
                { uid: '1', type: 'figma', state: 'started', createdAt: new Date('2024-01-01') },
                { uid: '2', type: 'figma', state: 'picked', createdAt: new Date('2024-01-02') },
                { uid: '3', type: 'figma', state: 'queued', createdAt: new Date('2024-01-03') },
            ];

            const handler = require('./jobs-pickup');
            await handler(createReq([{ type: 'figma' }]), createRes());

            expect(capturedStatus).toBe(200);
            expect(capturedBody).toEqual({});
        });
    });

    describe('when NEXRENDER_CONCURRENCY_LIMITS is not set', () => {
        it('should pick up jobs normally without concurrency restrictions', async () => {
            delete process.env.NEXRENDER_CONCURRENCY_LIMITS;
            jest.resetModules();

            mockData = [
                { uid: '1', type: 'figma', state: 'started', createdAt: new Date('2024-01-01') },
                { uid: '2', type: 'figma', state: 'queued', createdAt: new Date('2024-01-02') },
            ];

            const handler = require('./jobs-pickup');
            await handler(createReq([{ type: 'figma' }]), createRes());

            expect(capturedStatus).toBe(200);
            expect(capturedBody).toEqual(expect.objectContaining({ uid: '2', state: 'picked' }));
        });
    });
});
