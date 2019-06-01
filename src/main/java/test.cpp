TreeNode* R(vector<int> a, int abegin, int aend, vector<int> b, int bbegin, int bend) {

	if (abegin >= aend || bbegin >= bend)
		return NULL;
	TreeNode* root = new TreeNode(a[abegin]);
	int pivot;
	for (pivot = bbegin; pivot < bend; pivot++)
		if (a[abegin] = b[pivot])
			break;
	root->left = R(a, abegin + 1, abegin + pivot - bbegin + 1, b, bbegin, pivot);
	root->right = R(a, abegin + pivot - bbegin + 1, aend, b, pivot + 1, bend);
	return root;
}