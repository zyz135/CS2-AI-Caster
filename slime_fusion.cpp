#include <iostream>
#include <vector>
#include <algorithm>
#define ll long long
using namespace std;

void solve() {
    int N;
    if (!(cin >> N)) return;

    // Create circular array: duplicate the input
    vector<int> size(2 * N + 1);
    for (int i = 1; i <= N; ++i) {
        cin >> size[i];
        size[i + N] = size[i];
    }

    // dp[i][j] = maximum experience from fusing slimes from position i to j
    // Using 1-based indexing
    vector<vector<ll>> dp(2 * N + 1, vector<ll>(2 * N + 1, 0));

    // Fill DP table
    // For each possible end position
    for (int len = 2; len <= N; ++len) {
        for (int start = 1; start <= 2 * N; ++start) {
            int end = start + len - 1;
            if (end > 2 * N) continue;
            
            // Try all split points
            for (int k = start; k < end; ++k) {
                // When fusing [start, k] and [k+1, end]:
                // Left part: head = size[start], tail = size[k+1]
                // Right part: head = size[k+1], tail = size[end+1]
                // Fusion experience = size[start] * size[k+1] * size[end+1]
                ll energy = (ll)size[start] * size[k + 1] * size[end + 1];
                
                ll current_val = dp[start][k] + dp[k + 1][end] + energy;
                
                if (current_val > dp[start][end]) {
                    dp[start][end] = current_val;
                }
            }
        }
    }

    // Find maximum across all possible starting positions (circular)
    ll ans = 0;
    for (int i = 1; i <= N; ++i) {
        ans = max(ans, dp[i][i + N - 1]);
    }
    cout << ans << endl;
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(NULL);

    solve();

    return 0;
}
