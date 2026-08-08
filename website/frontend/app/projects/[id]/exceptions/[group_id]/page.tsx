'use client';

import { useEffect, useState } from 'react';
import { useRouter, useParams } from 'next/navigation';
import { projectsApi, ErrorGroupDetail, Error } from '@/lib/api';
import Link from 'next/link';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/cjs/styles/prism';
import { format } from 'date-fns';

export default function ExceptionGroupDetailPage() {
  const router = useRouter();
  const params = useParams();
  const projectId = params.id as string;
  const groupId = params.group_id as string;
  
  const [detail, setDetail] = useState<ErrorGroupDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (projectId && groupId) {
      loadDetail();
    }
  }, [projectId, groupId]);

  const loadDetail = async () => {
    try {
      const data = await projectsApi.getExceptionGroup(projectId, groupId);
      setDetail(data);
    } catch (err) {
      console.error('Failed to load exception group:', err);
      router.push(`/projects/${projectId}`);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <p>Loading...</p>
      </div>
    );
  }

  if (!detail) {
    return null;
  }

  const { group, recent_exceptions } = detail;
  const firstError = recent_exceptions[0];

  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between h-16">
            <div className="flex items-center space-x-4">
              <Link href={`/projects/${projectId}`} className="text-gray-600 hover:text-gray-900">
                ← Back
              </Link>
              <h1 className="text-2xl font-bold">Exception Details</h1>
            </div>
          </div>
        </div>
      </nav>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Summary */}
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <div className="flex items-start justify-between mb-4">
            <div>
              <h2 className="text-2xl font-bold mb-2">{group.message}</h2>
              {group.exception_type && (
                <p className="text-lg text-gray-600 mb-2">
                  <span className="font-semibold">{group.exception_type}</span>
                  {group.exception_value && `: ${group.exception_value}`}
                </p>
              )}
            </div>
            <div className="flex space-x-2">
              <span className={`px-3 py-1 rounded ${
                group.level === 'error' ? 'bg-red-100 text-red-800' :
                group.level === 'warning' ? 'bg-yellow-100 text-yellow-800' :
                'bg-blue-100 text-blue-800'
              }`}>
                {group.level}
              </span>
              <span className={`px-3 py-1 rounded ${
                group.status === 'unresolved' ? 'bg-red-100 text-red-800' :
                group.status === 'resolved' ? 'bg-green-100 text-green-800' :
                'bg-gray-100 text-gray-800'
              }`}>
                {group.status}
              </span>
            </div>
          </div>
          
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div>
              <span className="text-gray-500">Occurrences:</span>
              <span className="ml-2 font-semibold">{group.count}</span>
            </div>
            <div>
              <span className="text-gray-500">First Seen:</span>
              <span className="ml-2">{format(new Date(group.first_seen), 'MMM d, yyyy HH:mm')}</span>
            </div>
            <div>
              <span className="text-gray-500">Last Seen:</span>
              <span className="ml-2">{format(new Date(group.last_seen), 'MMM d, yyyy HH:mm')}</span>
            </div>
          </div>
        </div>

        {/* Stack Trace */}
        {firstError?.stacktrace && Array.isArray(firstError.stacktrace) && firstError.stacktrace.length > 0 && (
          <div className="bg-white rounded-lg shadow p-6 mb-6">
            <h3 className="text-lg font-semibold mb-4">Stack Trace</h3>
            <div className="overflow-x-auto">
              <SyntaxHighlighter
                language="javascript"
                style={vscDarkPlus}
                customStyle={{ borderRadius: '8px' }}
              >
                {firstError.stacktrace
                  .map((frame: any) => {
                    const file = frame.filename || 'unknown';
                    const func = frame.function || 'unknown';
                    const line = frame.lineno || '?';
                    return `at ${func} (${file}:${line})`;
                  })
                  .join('\n')}
              </SyntaxHighlighter>
            </div>
          </div>
        )}

        {/* Context */}
        {firstError?.context && Object.keys(firstError.context).length > 0 && (
          <div className="bg-white rounded-lg shadow p-6 mb-6">
            <h3 className="text-lg font-semibold mb-4">Context</h3>
            <pre className="bg-gray-50 p-4 rounded overflow-x-auto">
              {JSON.stringify(firstError.context, null, 2)}
            </pre>
          </div>
        )}

        {/* Tags */}
        {firstError?.tags && Object.keys(firstError.tags).length > 0 && (
          <div className="bg-white rounded-lg shadow p-6 mb-6">
            <h3 className="text-lg font-semibold mb-4">Tags</h3>
            <div className="flex flex-wrap gap-2">
              {Object.entries(firstError.tags).map(([key, value]) => (
                <span key={key} className="px-3 py-1 bg-gray-100 rounded text-sm">
                  {key}: {String(value)}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Recent Occurrences */}
        <div className="bg-white rounded-lg shadow">
          <div className="px-6 py-4 border-b">
            <h3 className="text-lg font-semibold">Recent Occurrences</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Timestamp</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">User</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {recent_exceptions.map((error) => (
                  <tr key={error.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 text-sm">
                      {format(new Date(error.timestamp), 'MMM d, yyyy HH:mm:ss')}
                    </td>
                    <td className="px-6 py-4 text-sm">
                      {error.user_data && typeof error.user_data === 'object' && 'id' in error.user_data
                        ? String(error.user_data.id)
                        : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </main>
    </div>
  );
}
