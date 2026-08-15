import { useState, type FormEvent } from 'react';
import { useAuthStore } from '../../stores/authStore';

export function LoginPage() {
  const [password, setPassword] = useState('');
  const { login, loading, error } = useAuthStore();

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    login(password);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-bg">
      <form
        onSubmit={handleSubmit}
        className="bg-surface-raised p-8 rounded-lg border border-border-subtle w-80"
      >
        <h1 className="text-xl font-semibold mb-6 text-center">Nerve</h1>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          autoFocus
          className="w-full px-3 py-2 bg-surface-raised border border-border-subtle rounded text-text outline-none focus:border-accent mb-4"
        />
        {error && <p className="text-hue-red text-sm mb-3">{error}</p>}
        <button
          type="submit"
          disabled={loading}
          className="w-full py-2 bg-accent hover:bg-accent-hover text-on-accent rounded font-medium disabled:opacity-50 cursor-pointer"
        >
          {loading ? '...' : 'Login'}
        </button>
      </form>
    </div>
  );
}
