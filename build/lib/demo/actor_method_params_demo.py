#!/usr/bin/env python3
"""
Demo showing how to pass parameters to actor methods when using submit_actor.
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

    def train(self, epochs=1, batch_size=32, optimizer="adam"):
        """Simulate training process with multiple parameters."""
        print(f"Training {self.model_name} for {epochs} epochs with batch size {batch_size} using {optimizer}")
        self.training_steps += epochs
        # Simulate some training work
        import time
        time.sleep(0.5)  # Simulate training time
        return {
            "model": self.model_name,
            "steps_completed": self.training_steps,
            "epochs": epochs,
            "batch_size": batch_size,
            "optimizer": optimizer,
            "learning_rate": self.learning_rate,
            "status": "training_complete"
        }

    def evaluate(self, dataset="validation", metrics=["accuracy"]):
        """Simulate evaluation process."""
        print(f"Evaluating {self.model_name} on {dataset} with metrics: {metrics}")
        import time
        time.sleep(0.3)  # Simulate evaluation time
        return {
            "model": self.model_name,
            "dataset": dataset,
            "metrics": metrics,
            "accuracy": 0.95,  # Simulated accuracy
            "loss": 0.05       # Simulated loss
        }

    def get_status(self):
        """Get current training status."""
        return {
            "model": self.model_name,
            "learning_rate": self.learning_rate,
            "training_steps": self.training_steps
        }


def demo_actor_method_parameters():
    """Demonstrate how to pass parameters to actor methods."""
    print("=== Actor Method Parameter Passing Demo ===\n")

    try:
        # 1. Initialize the scheduler environment
        print("1. Initializing scheduler environment...")
        task_lifecycle_manager = initialize_scheduler_environment()
        print("✅ Scheduler environment initialized successfully\n")

        # 2. Submit an actor to the scheduler
        print("2. Submitting Train actor to scheduler...")
        actor_id, actor_instance = submit_actor(
            actor_class=Train,
            kwargs={"model_name": "resnet50", "learning_rate": 0.001},
            name="resnet50_trainer"
        )
        print(f"✅ Actor submitted successfully!\n")

        # 3. 调用Actor方法并传递参数
        print("3. Calling actor methods with parameters...")

        # 3.1 调用train方法并传递多个参数
        print("   a) Calling train() method with parameters:")
        train_result_ref = actor_instance.train.remote(
            epochs=10,
            batch_size=128,
            optimizer="sgd"
        )
        train_result = ray.get(train_result_ref)
        print(f"      Train result: {train_result}\n")

        # 3.2 调用evaluate方法并传递参数
        print("   b) Calling evaluate() method with parameters:")
        eval_result_ref = actor_instance.evaluate.remote(
            dataset="test",
            metrics=["accuracy", "precision", "recall"]
        )
        eval_result = ray.get(eval_result_ref)
        print(f"      Evaluation result: {eval_result}\n")

        # 3.3 调用不带参数的方法
        print("   c) Calling get_status() method without parameters:")
        status_ref = actor_instance.get_status.remote()
        status = ray.get(status_ref)
        print(f"      Status: {status}\n")

        # 4. 异步调用示例
        print("4. Async method calls example:")
        # 同时发起多个异步调用
        train_refs = []
        for i in range(3):
            ref = actor_instance.train.remote(epochs=1, batch_size=64, optimizer=f"adam_{i}")
            train_refs.append(ref)

        # 等待所有结果
        results = ray.get(train_refs)
        print(f"   Completed {len(results)} training sessions:")
        for i, result in enumerate(results):
            print(f"     Session {i+1}: {result['epochs']} epochs, optimizer: {result['optimizer']}")

        print("\n🎉 Actor method parameter passing demo completed successfully!")

    except Exception as e:
        print(f"❌ Error in demo: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    success = demo_actor_method_parameters()
    if success:
        print("\n✅ Actor method parameter passing demo finished successfully!")
    else:
        print("\n💥 Actor method parameter passing demo failed!")