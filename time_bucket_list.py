# All the math comes from https://www.johndcook.com/blog/standard_deviation/ which sites
# The Art of Computer Programming by Donald Knuth and a paper by B. P. Welford
class RunningVariance:
    def __init__(self):
        self.n = 0
        self.oldM = 0.0
        self.oldS = 0.0
        self.newM = 0.0
        self.newS = 0.0

    def append(self, value):
        self.n += 1
        if self.n == 1:
            self.oldM = value
            self.newM = value
            self.oldS = 0.0
        else:
            self.newM = self.oldM + (value - self.oldM) / self.n
            self.newS = self.oldS + (value - self.oldM) * (value - self.newM)
            self.oldM, self.oldS = self.newM, self.newS

    def number_of_buckets(self):
        return self.n

    def mean(self):
        return self.newM

    def variance(self):
        if self.n > 1:
            return self.newS / (self.n - 1)
        return 0.0

    def standard_deviation(self):
        return self.variance() ** 0.5


import logging


class TimeBucketList:
    def __init__(self, bucket_size):
        self.running_variance = RunningVariance()
        self.last_bucket_aggregate = 0
        self.current_bucket_aggregate = 0
        self.current_bucket = 0
        self.bucket_size = bucket_size

    def append(self, timestamp):
        bucket = timestamp // self.bucket_size

        # Hack to reset anomaly detection for new streams until we have a better way to clear this when streams go offline
        if bucket - self.current_bucket > 60:
            self.running_variance = RunningVariance()
            self.last_bucket_aggregate = 0
            self.current_bucket_aggregate = 0
            self.current_bucket = 0

        if self.current_bucket == 0:
            self.current_bucket = bucket

        if bucket == self.current_bucket:
            self.current_bucket_aggregate += 1
            return

        # It's possible that we haven't received a message in a long time. If that's the case,
        # we need to append 0s for all the empty buckets
        for _ in range(self.current_bucket, bucket):
            self.running_variance.append(0)

        # We just crossed over into a new bucket. Record the previous bucket.
        self.running_variance.append(self.current_bucket_aggregate)

        # Now update the bucket to reflect that we started a new bucket
        self.current_bucket = bucket
        self.last_bucket_aggregate = self.current_bucket_aggregate
        self.current_bucket_aggregate = 1

    def check_for_anomaly(self):
        threshold = self.running_variance.standard_deviation() * 5
        anomaly_detected = self.last_bucket_aggregate > threshold
        if anomaly_detected:
            ratio = (
                self.last_bucket_aggregate / self.running_variance.standard_deviation()
            )
            logging.error(f"RATIO: {ratio}")
        return anomaly_detected

    def append_and_check_for_anomaly(self, timestamp):
        self.append(timestamp)
        return self.check_for_anomaly()

    def size(self):
        return self.running_variance.number_of_buckets()
