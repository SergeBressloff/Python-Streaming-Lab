"""
consumer.py
----------------
This file contains the logic for a Consumer that continuously reads messages
from the queue and processes them.

The Consumer runs forever, just like a streaming system.
"""

import time
from queue import Queue

class Consumer:
    def __init__(self, q, min_amount=None, max_amount=None, user_ids=None):
        """
        PARAMETERS:
        - q          : the shared queue (simulated topic)
        - min_amount : optional; skip events with amount below this value
        - max_amount : optional; skip events with amount above this value
        - user_ids   : optional; only keep events from these user IDs

        Any filter left as None is disabled, so Consumer(q) processes everything.

        TODO:
        - Store the queue.
        - Initialize any state variables you may want later,
          e.g. a running total for stateful computations.
        """
        self.q = q
        self.min_amount = min_amount
        self.max_amount = max_amount
        self.user_ids = set(user_ids) if user_ids is not None else None

        self.running_total = 0
        self.processed_count = 0
        self.filtered_count = 0

    def start(self):
        """
        Main loop of the consumer.

        TODO:
        - Continuously call q.get() to receive events.
        - Print the event received.
        - Pass it to the process() function.
        """
        while True:
            event = self.q.get()
            if event is None:  # Check for the termination signal
                print("No more events to process. Exiting.")
                break
            print(f"Received event: {event}")
            self.process(event)

    def process(self, event):
        """
        TODO:
        - Simulate some processing time using time.sleep().
        - Transform fields (e.g., convert amount to float).
        - Implement optional filtering conditions.
        - Update any state (example: running total).
        """
        time.sleep(0.5)  # Simulate processing time

        # Transform: CSV values arrive as strings, so convert them to numbers
        try:
            amount = float(event['amount'])
            user_id = int(event['user_id'])
        except (KeyError, ValueError) as e:
            self.filtered_count += 1
            print(f"Skipped malformed event ({e!r}): {event}")
            return

        # Optional filters: each one is only applied if it was configured
        reason = self.filter_reason(amount, user_id)
        if reason:
            self.filtered_count += 1
            print(f"Filtered out event {event['transaction_id']}: {reason}. Running total: {self.running_total:.2f} "
                  f"(processed={self.processed_count}, filtered={self.filtered_count})")
            return

        # Update state
        self.running_total += amount
        self.processed_count += 1
        print(f"Processed event. Running total: {self.running_total:.2f} "
              f"(processed={self.processed_count}, filtered={self.filtered_count})")

    def filter_reason(self, amount, user_id):
        """
        Return why an event should be dropped, or None if it passes all filters.
        """
        if self.min_amount is not None and amount < self.min_amount:
            return f"amount {amount} < min_amount {self.min_amount}"
        if self.max_amount is not None and amount > self.max_amount:
            return f"amount {amount} > max_amount {self.max_amount}"
        if self.user_ids is not None and user_id not in self.user_ids:
            return f"user_id {user_id} not in {sorted(self.user_ids)}"
        return None


# Debugging test

if __name__ == "__main__":
    """
    Run this alone to see consumer behavior.

    Note: It will block waiting for messages since no producer is running.
    """
    q = Queue()
    consumer = Consumer(q)
    consumer.start()
