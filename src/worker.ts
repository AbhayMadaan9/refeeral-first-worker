import 'dotenv/config';
import { createServer } from 'node:http';
import { Worker } from 'bullmq';
import { prisma } from './db.js';
import { redis, notificationQueue, enqueueNotification } from './queue.js';
import { localDayEnd, localJobDate, scheduleSameDay } from './time.js';

function logWorkerEvent(level: 'info' | 'error', event: string, details: Record<string, unknown> = {}) {
  const payload = JSON.stringify({ timestamp: new Date().toISOString(), service: 'notification-worker', event, ...details });
  if (level === 'error') console.error(payload);
  else console.log(payload);
}

const port = Number(process.env.PORT ?? 3001);
const healthServer = createServer((request, response) => {
  if (request.url === '/' || request.url === '/health') {
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify({ ok: true, service: 'notification-worker' }));
    return;
  }
  response.writeHead(404, { 'Content-Type': 'application/json' });
  response.end(JSON.stringify({ error: 'Not found' }));
});

healthServer.listen(port, '0.0.0.0', () => {
  logWorkerEvent('info', 'worker.health_server.started', { host: '0.0.0.0', port });
});

const worker = new Worker('referral-notifications', async queueJob => {
  const { notificationId } = queueJob.data as { notificationId: string };
  const notification = await prisma.notification.findUnique({ include: { job: { include: { user: true } } }, where: { id: notificationId } });
  if (!notification || notification.status !== 'SCHEDULED') return;
  const now = new Date();
  const job = notification.job;
  const jobDate = job.jobDate.toISOString().slice(0, 10);
  const currentDate = localJobDate(job.user.timezone, now);
  const cancelReason = job.deletedAt
    ? 'job_deleted'
    : job.referralStatus === 'RECEIVED'
      ? 'referral_received'
      : jobDate !== currentDate
        ? 'job_archived'
        : now >= localDayEnd(job.user.timezone, now)
          ? 'local_day_ended'
          : null;
  if (cancelReason) {
    await prisma.notification.update({ where: { id: notification.id }, data: { status: 'CANCELLED' } });
    return;
  }
  if (notification.type === 'APPLY_DIRECTLY' && job.applyDirectNotificationSentAt) {
    await prisma.notification.update({ where: { id: notification.id }, data: { status: 'CANCELLED' } });
    return;
  }
  if (notification.type === 'FOLLOW_UP' && (
    job.applyDirectNotificationSentAt ||
    (job.applyDirectNotificationAt && (now >= job.applyDirectNotificationAt || notification.scheduledAt >= job.applyDirectNotificationAt))
  )) {
    await prisma.notification.update({ where: { id: notification.id }, data: { status: 'CANCELLED' } });
    await prisma.job.update({ where: { id: job.id }, data: { nextFollowUpAt: null } });
    return;
  }
  await prisma.notification.update({ where: { id: notification.id }, data: { status: 'SENT', sentAt: now } });
  if (notification.type === 'APPLY_DIRECTLY') {
    const pendingFollowUps = await prisma.notification.findMany({ where: { jobId: job.id, type: 'FOLLOW_UP', status: 'SCHEDULED' } });
    await prisma.notification.updateMany({ where: { jobId: job.id, type: 'FOLLOW_UP', status: 'SCHEDULED' }, data: { status: 'CANCELLED' } });
    await prisma.job.update({ where: { id: job.id }, data: { applyDirectNotificationSentAt: now, nextFollowUpAt: null } });
    await Promise.all(pendingFollowUps.map(async followUp => {
      try {
        const queuedJob = await notificationQueue.getJob(`notification-${followUp.id}`);
        if (queuedJob) await queuedJob.remove();
      } catch (error) {
        logWorkerEvent('error', 'follow_up_queue.remove_failed', {
          notificationId: followUp.id,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }));
    return;
  }
  const nextCandidate = scheduleSameDay(now, job.user.followUpIntervalMinutes / 60, job.user.timezone);
  const next = nextCandidate && (!job.applyDirectNotificationAt || nextCandidate < job.applyDirectNotificationAt)
    ? nextCandidate
    : null;
  if (next) {
    const followUp = await prisma.notification.create({ data: { userId: job.userId, jobId: job.id, type: 'FOLLOW_UP', scheduledAt: next } });
    await enqueueNotification(followUp.id, next);
  }
  await prisma.job.update({ where: { id: job.id }, data: { nextFollowUpAt: next } });
}, { connection: redis });

worker.on('failed', (queueJob, error) => logWorkerEvent('error', 'queue_job.failed', { queueJobId: queueJob?.id, notificationId: (queueJob?.data as { notificationId?: string } | undefined)?.notificationId, attemptsMade: queueJob?.attemptsMade, error: error.message }));
worker.on('stalled', jobId => logWorkerEvent('error', 'queue_job.stalled', { queueJobId: jobId }));
worker.on('error', error => logWorkerEvent('error', 'worker.error', { error: error.message }));
logWorkerEvent('info', 'worker.started', { queue: 'referral-notifications' });

async function recoverScheduledNotifications() {
  const pending = await prisma.notification.findMany({
    where: { status: 'SCHEDULED' },
    include: { job: { include: { user: true } } }
  });
  for (const notification of pending) {
    const job = notification.job;
    const isActiveToday = !job.deletedAt && job.referralStatus === 'REQUESTED'
      && job.jobDate.toISOString().slice(0, 10) === localJobDate(job.user.timezone);
    if (!isActiveToday) {
      await prisma.notification.update({ where: { id: notification.id }, data: { status: 'CANCELLED' } });
      continue;
    }
    await enqueueNotification(notification.id, notification.scheduledAt);
  }
  logWorkerEvent('info', 'notification.recovery_complete', { requeuedCount: pending.length });
}

void recoverScheduledNotifications().catch(error => {
  logWorkerEvent('error', 'notification.recovery_failed', { error: error instanceof Error ? error.message : String(error) });
});
