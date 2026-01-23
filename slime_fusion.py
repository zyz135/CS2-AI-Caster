def solve_slime_fusion(N, heads):
    """
    史莱姆大融合问题
    使用区间DP解决环形问题
    
    参数:
        N: 史莱姆的数量
        heads: 每个史莱姆的头围列表
    
    返回:
        最大总经验值
    """
    # 构建完整的数组（处理环形）
    # 对于环形问题，我们可以将数组复制一份，然后取长度为N的区间
    extended_heads = heads + heads
    
    # dp[i][j] 表示从位置i到位置j（包含）的史莱姆融合后的最大经验值
    # 同时记录融合后的头围和尾围
    # 由于是环形，我们需要考虑所有可能的起始位置
    
    max_experience = 0
    
    # 尝试每个可能的起始位置（因为环形）
    for start in range(N):
        # 构建当前区间的头围数组
        current_heads = extended_heads[start:start+N]
        
        # dp[i][j] = (max_exp, head, tail)
        # 表示从i到j的史莱姆融合后的最大经验值，以及融合后的头围和尾围
        dp = [[(0, 0, 0) for _ in range(N)] for _ in range(N)]
        
        # 初始化：单个史莱姆
        for i in range(N):
            head = current_heads[i]
            # 尾围是下一个史莱姆的头围（环形）
            tail = current_heads[(i + 1) % N]
            dp[i][i] = (0, head, tail)
        
        # 区间长度从2到N
        for length in range(2, N + 1):
            for i in range(N - length + 1):
                j = i + length - 1
                max_exp = 0
                best_head = 0
                best_tail = 0
                
                # 尝试所有分割点k，将区间[i,j]分割为[i,k]和[k+1,j]
                for k in range(i, j):
                    # 左半部分：[i, k]
                    left_exp, left_head, left_tail = dp[i][k]
                    # 右半部分：[k+1, j]
                    right_exp, right_head, right_tail = dp[k+1][j]
                    
                    # 检查是否可以融合（左半部分的尾围应该等于右半部分的头围）
                    if left_tail == right_head:
                        # 融合后的新史莱姆：(left_head, right_tail)
                        # 获得的经验值：left_head * left_tail * right_tail
                        fusion_exp = left_head * left_tail * right_tail
                        total_exp = left_exp + right_exp + fusion_exp
                        
                        if total_exp > max_exp:
                            max_exp = total_exp
                            best_head = left_head
                            best_tail = right_tail
                
                dp[i][j] = (max_exp, best_head, best_tail)
        
        # 更新全局最大经验值
        if dp[0][N-1][0] > max_experience:
            max_experience = dp[0][N-1][0]
    
    return max_experience


def main():
    """主函数：处理输入和输出"""
    # 读取输入
    N = int(input().strip())
    heads = list(map(int, input().split()))
    
    # 求解
    result = solve_slime_fusion(N, heads)
    
    # 输出结果
    print(result)


if __name__ == "__main__":
    main()

