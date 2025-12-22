#!/usr/bin/env python3
"""
Demo script showing how to submit Ray Actors to the multicluster scheduler
using the unified scheduler interface.
"""

import ray
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_actor
)


@ray.remote
class Train:
    """Example training actor class."""

    def __init__(self, model_name="default_model", learning_rate=0.01):
        """Initialize the training actor."""
        self.model_name = model_name
        self.learning_rate = learning_rate
        self.training_steps = 0
        print(f"Train actor initialized with model: {self.model_name}, lr: {self.learning_rate}")

    def train(self, epochs=1, batch_size=32):
        """Simulate training process."""
        print(f"Training {self.model_name} for {epochs} epochs with batch size {batch_size}")
        self.training_steps += epochs
        # Simulate some training work
        import time
        time.sleep(1)  # Simulate training time
        return {
            "model": self.model_name,
            "steps_completed": self.training_steps,
            "status": "training_complete"
        }

    def get_status(self):
        """Get current training status."""
        return {
            "model": self.model_name,
            "learning_rate": self.learning_rate,
            "training_steps": self.training_steps
        }


def demo_actor_submission():
    """Demonstrate how to submit actors to the multicluster scheduler."""
    print("=== Ray Multicluster Scheduler Actor Submission Demo ===\n")

    try:
        # 1. Initialize the scheduler environment
        print("1. Initializing scheduler environment...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ Scheduler environment initialized successfully\n")

        # 2. Submit an actor to the scheduler using unified interface
        print("2. Submitting Train actor to scheduler...")
        actor_id, actor_instance = submit_actor(
            actor_class=Train,
            args=(),  # 不要在args中传递model_name参数
            kwargs={"model_name": "resnet50", "learning_rate": 0.001},
            resource_requirements={"CPU": 1},
            tags=["training", "computer_vision"],
            name="resnet50_trainer",
            preferred_cluster="mac"  # Specify preferred cluster
        )
        print(f"✅ Actor submitted successfully!")
        print(f"   Actor ID: {actor_id}")
        print(f"   Actor Instance: {actor_instance}\n")

        # 3. Use the actor instance to call methods
        print("3. Calling methods on the actor...")

        # Get initial status
        status_ref = actor_instance.get_status.remote()
        status = ray.get(status_ref)
        print(f"   Initial status: {status}")

        # Perform training
        train_result_ref = actor_instance.train.remote(epochs=5, batch_size=64)
        train_result = ray.get(train_result_ref)
        print(f"   Training result: {train_result}")

        # Get updated status
        updated_status_ref = actor_instance.get_status.remote()
        updated_status = ray.get(updated_status_ref)
        print(f"   Updated status: {updated_status}\n")

        # 4. Submit another actor without specifying a preferred cluster
        print("4. Submitting another actor without preferred cluster...")
        actor_id2, actor_instance2 = submit_actor(
            actor_class=Train,
            args=(),  # 不要在args中传递参数
            kwargs={"model_name": "bert", "learning_rate": 0.0001},
            resource_requirements={"CPU": 1},
            tags=["nlp", "transformer"],
            name="bert_trainer"
            # No preferred_cluster specified, will use scheduler's default logic
        )
        print(f"✅ Second actor submitted successfully!")
        print(f"   Actor ID: {actor_id2}")
        print(f"   Actor Instance: {actor_instance2}\n")

        # 5. Use the second actor
        print("5. Using the second actor...")
        bert_status_ref = actor_instance2.get_status.remote()
        bert_status = ray.get(bert_status_ref)
        print(f"   BERT trainer status: {bert_status}")

        print("\n🎉 Actor submission demo completed successfully!")

    except Exception as e:
        print(f"❌ Error in demo: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    success = demo_actor_submission()
    if success:
        print("\n✅ Actor submission demo finished successfully!")
        exit(0)
    else:
        print("\n💥 Actor submission demo failed!")
        exit(1)