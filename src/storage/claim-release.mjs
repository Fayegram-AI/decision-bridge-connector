/** Restore only a claim whose invocation marker was never committed. */
export async function restoreUnstartedClaim(tx, job) {
    const reservation = job.unstartedClaim;
    if (!reservation) return false;
    if (reservation.rateKey) {
        const rate = await tx.get('controls', reservation.rateKey, true);
        if (rate?.token === job.token) await tx.remove('controls', reservation.rateKey);
    }
    job.state = reservation.state;
    if (reservation.worker === undefined) delete job.worker;
    else job.worker = reservation.worker;
    job.token = null;
    job.leaseUntil = null;
    delete job.unstartedClaim;
    return true;
}
