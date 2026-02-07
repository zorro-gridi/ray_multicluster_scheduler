# 问题发现
- 1. _prepare_runtime_env_for_cluster_target 没有获取到用户的 runtime_env 参数

# 用户集群配置 yaml
clusters:
  # CentOS cluster
  - name: centos
    head_address: 192.168.5.7:32546
    dashboard: http://192.168.5.7:31591
    prefer: false
    weight: 1.0
    runtime_env:
      conda: ts
      working_dir: .
      excludes:
        - '*.pkl'
        - '*.db'
        - "/OKX_Open_API_SDK_V5/"
        - '*.csv'
        - '*.log'
        - "/FundAnalysis/"
        - "/.idea/"
        - "/XueQiu/"
        - "/Index_Coeffiecient/"
      env_vars:
        home_dir: /home/zorro/
        PYTHONPATH: |
          /home/zorro/project/pycharm/Fund:
          /home/zorro/project/pycharm/rlops:
          /home/zorro/project/pycharm/mlops:
          /home/zorro/project/pycharm/tools:
          /home/zorro/project/pycharm/ray_multicluster_scheduler:
          ${PYTHONPATH}
    tags:
      - linux
      - x86_64

  # macOS cluster
  - name: mac
    head_address: 127.0.0.1:32546
    dashboard: http://127.0.0.1:8265
    prefer: true
    weight: 1.2
    runtime_env:
      conda: k8s
      working_dir: .
      excludes:
        - '*.pkl'
        - '*.db'
        - "/OKX_Open_API_SDK_V5"
        - '*.csv'
        - '*.log'
        - "/FundAnalysis/"
        - "/.idea/"
        - "/XueQiu/"
        - "/Index_Coeffiecient/"
      env_vars:
        home_dir: /Users/zorro/
        PYTHONPATH: |
          /Users/zorro/project/pycharm/Fund:
          /Users/zorro/project/pycharm/rlops:
          /Users/zorro/project/pycharm/mlops:
          /Users/zorro/project/pycharm/tools:
          /Users/zorro/project/pycharm/ray_multicluster_scheduler:
          ${PYTHONPATH}
    tags:
      - macos
      - arm64

# 控制台输出
2025-12-29 14:05:14,919 WARNING packaging.py:405 -- File /Users/zorro/project/pycharm/Fund/Fund_Forecast_Return/models/model_of_H11140.pkl is very large (24.66MiB). Consider adding this file to the 'excludes' list to skip uploading it: `ray.init(..., runtime_env={'excludes': ['/Users/zorro/project/pycharm/Fund/Fund_Forecast_Return/models/model_of_H11140.pkl']})`

# 任务需求
- 1. 检查 runtime_env 的参数传递过程，是否遗漏